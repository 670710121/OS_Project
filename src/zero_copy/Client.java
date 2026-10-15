package zero_copy;

import java.io.*;
import java.net.InetSocketAddress;
import java.nio.channels.FileChannel;
import java.nio.channels.SocketChannel;
import java.nio.file.*;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;

/**
 * Client: แบ่งไฟล์เป็นช่วง ๆ ดาวน์โหลดพร้อมกันด้วยหลาย worker แล้ว merge
 *
 * วิธีรัน:  java Client <ชื่อไฟล์> <io|nio> [จำนวน workers]
 * ตัวอย่าง:  javac zero_copy/*.java
 *          java zero_copy/Server io
 *          java zero_copy/Server nio
 *          java zero_copy/Client big.bin io 10
 *          java zero_copy/Client big.bin nio 1
 *          fsutil file createnew zero_copy\shared\big.bin 524288000
 */
public class Client {
    static final String HOST = "localhost";
    static final int PORT = 5000;
    static final Path OUT_DIR = Paths.get("downloads");   // ผลดาวน์โหลดไปอยู่ที่นี่
    static final Path SRC_DIR = Paths.get("files");       // โฟลเดอร์ต้นทางที่ Server ใช้ (ต้องตรงกับ Server.DIR)
    static final long TEST_FILE_SIZE = 100L * 1024 * 1024; // ขนาดไฟล์ทดสอบ 100 MB

    public static void main(String[] args) throws Exception {
        String name = args[0];
        boolean useNio = args[1].equalsIgnoreCase("nio");
        int workers = args.length > 2 ? Integer.parseInt(args[2]) : 10;

        Files.createDirectories(OUT_DIR);
        ensureTestFile(name);   // สร้างโฟลเดอร์ files และไฟล์ทดสอบให้ ถ้ายังไม่มี

        // ---------- ขั้น 1: ถาม Server (LIST + INFO) ----------
        long size = askServer(name);
        System.out.println("File size = " + size + " bytes, workers = " + workers
                + ", mode = " + (useNio ? "NIO" : "Traditional I/O"));

        long start = System.nanoTime();   // เริ่มจับเวลา

        // ---------- ขั้น 2: คำนวณช่วง byte ของแต่ละ worker ----------
        // ช่วงไม่ซ้อนกัน: worker i เริ่มที่ i * partSize, worker สุดท้ายรับเศษที่เหลือ
        long partSize = size / workers;
        ExecutorService pool = Executors.newFixedThreadPool(workers);   // 1 worker = 1 thread
        List<Future<?>> futures = new ArrayList<>();
        List<Path> partFiles = new ArrayList<>();

        for (int i = 0; i < workers; i++) {
            long offset = i * partSize;
            long length = (i == workers - 1) ? size - offset : partSize;   // ตัวสุดท้ายรับที่เหลือ
            Path partFile = OUT_DIR.resolve(name + ".part" + i);
            partFiles.add(partFile);

            // ---------- ขั้น 3: แต่ละ worker โหลดช่วงของตัวเอง ----------
            futures.add(pool.submit(() -> {
                try {
                    downloadPart(name, offset, length, partFile, useNio);
                } catch (IOException e) {
                    throw new UncheckedIOException(e);
                }
            }));
        }

        for (Future<?> f : futures) f.get();   // รอให้ worker ทุกตัวเสร็จ (ถ้ามีตัวใดพัง จะ throw ตรงนี้)
        pool.shutdown();

        // ---------- ขั้น 4: merge part files เป็นไฟล์สุดท้าย ----------
        Path finalFile = OUT_DIR.resolve("downloaded_" + name);
        merge(partFiles, finalFile);

        double seconds = (System.nanoTime() - start) / 1e9;
        double mb = size / (1024.0 * 1024.0);

        // ---------- ขั้น 5: ตรวจผล ----------
        System.out.printf("Time = %.3f s, Throughput = %.2f MB/s%n", seconds, mb / seconds);
        System.out.println("Size check: " + (Files.size(finalFile) == size ? "OK" : "MISMATCH"));
        System.out.println("SHA-256   : " + sha256(finalFile));   // เอาไปเทียบกับ sha256sum ของไฟล์ต้นฉบับ
    }

    // สร้างโฟลเดอร์ files และไฟล์ทดสอบ (ข้อมูลสุ่ม) ถ้ายังไม่มี
    // หมายเหตุ: ต้องรัน Client และ Server จากโฟลเดอร์เดียวกัน เพื่อให้ "files" เป็นที่เดียวกัน
    static void ensureTestFile(String name) throws IOException {
        Files.createDirectories(SRC_DIR);
        Path file = SRC_DIR.resolve(name);
        if (Files.exists(file)) return;                    // มีอยู่แล้ว ไม่ต้องสร้างซ้ำ

        System.out.println("Creating test file " + file + " (" + (TEST_FILE_SIZE / (1024 * 1024)) + " MB)...");
        java.util.Random rnd = new java.util.Random();
        byte[] buf = new byte[64 * 1024];
        try (OutputStream out = new BufferedOutputStream(new FileOutputStream(file.toFile()))) {
            long left = TEST_FILE_SIZE;
            while (left > 0) {
                rnd.nextBytes(buf);
                int n = (int) Math.min(buf.length, left);
                out.write(buf, 0, n);
                left -= n;
            }
        }
    }

    // ถามรายชื่อไฟล์ (LIST) และขนาดไฟล์ (INFO) ผ่าน connection แรก
    static long askServer(String name) throws IOException {
        try (SocketChannel ch = SocketChannel.open(new InetSocketAddress(HOST, PORT))) {
            DataOutputStream out = new DataOutputStream(ch.socket().getOutputStream());
            DataInputStream in = new DataInputStream(ch.socket().getInputStream());

            out.writeUTF("LIST");
            String line;
            System.out.println("Files on server:");
            while (!(line = in.readUTF()).equals("END")) {
                System.out.println("  " + line);
            }

            out.writeUTF("INFO " + name);
            String resp = in.readUTF();                    // "SIZE 12345" หรือ "ERROR ..."
            if (!resp.startsWith("SIZE ")) throw new IOException("Server said: " + resp);
            return Long.parseLong(resp.substring(5));
        }
    }

    // worker 1 ตัว: เปิด connection ของตัวเอง ขอ GET ช่วงของตัวเอง แล้วเก็บลง part file
    static void downloadPart(String name, long offset, long length, Path partFile, boolean useNio)
            throws IOException {
        try (SocketChannel ch = SocketChannel.open(new InetSocketAddress(HOST, PORT))) {
            DataOutputStream out = new DataOutputStream(ch.socket().getOutputStream());
            DataInputStream in = new DataInputStream(ch.socket().getInputStream());

            out.writeUTF("GET " + name + " " + offset + " " + length);
            String resp = in.readUTF();                    // "OK <len>" หรือ "ERROR ..."
            if (!resp.startsWith("OK")) throw new IOException("Server said: " + resp);

            if (useNio) {
                receiveWithNio(ch, partFile, length);
            } else {
                receiveWithTraditionalIO(in, partFile, length);
            }
        }
    }

    // ---------- วิธีที่ 1: Traditional I/O ----------
    // อ่านจาก socket เข้า byte[] แล้วเขียนลงไฟล์
    static void receiveWithTraditionalIO(InputStream in, Path partFile, long length)
            throws IOException {
        try (OutputStream fileOut = new FileOutputStream(partFile.toFile())) {
            byte[] buf = new byte[64 * 1024];
            long left = length;
            while (left > 0) {
                int n = in.read(buf, 0, (int) Math.min(buf.length, left));
                if (n < 0) throw new EOFException("Server closed early");
                fileOut.write(buf, 0, n);
                left -= n;
            }
        }
    }

    // ---------- วิธีที่ 2: NIO transferFrom ----------
    // สั่งให้ OS ย้ายข้อมูลจาก socket ลงไฟล์โดยตรง ไม่ผ่าน byte[] ใน Java
    static void receiveWithNio(SocketChannel ch, Path partFile, long length) throws IOException {
        try (FileChannel fc = FileChannel.open(partFile,
                StandardOpenOption.CREATE, StandardOpenOption.WRITE,
                StandardOpenOption.TRUNCATE_EXISTING)) {
            long pos = 0;                                   // ตำแหน่งในไฟล์ part (เริ่มที่ 0)
            while (pos < length) {
                long n = fc.transferFrom(ch, pos, length - pos);   // อาจได้น้อยกว่าที่ขอ จึงต้องวน
                if (n <= 0) throw new EOFException("Server closed early");
                pos += n;
            }
        }
    }

    // รวม part0, part1, ... ตามลำดับ เป็นไฟล์เดียว แล้วลบ part ทิ้ง
    static void merge(List<Path> parts, Path finalFile) throws IOException {
        try (OutputStream out = new FileOutputStream(finalFile.toFile())) {
            for (Path part : parts) {
                Files.copy(part, out);       // ต่อท้ายไฟล์ปลายทาง
                Files.delete(part);
            }
        }
    }

    // คำนวณ SHA-256 เพื่อตรวจว่าไฟล์ตรงกับต้นฉบับ
    static String sha256(Path file) throws Exception {
        MessageDigest md = MessageDigest.getInstance("SHA-256");
        try (InputStream in = new FileInputStream(file.toFile())) {
            byte[] buf = new byte[64 * 1024];
            int n;
            while ((n = in.read(buf)) > 0) md.update(buf, 0, n);
        }
        StringBuilder sb = new StringBuilder();
        for (byte b : md.digest()) sb.append(String.format("%02x", b));
        return sb.toString();
    }
}