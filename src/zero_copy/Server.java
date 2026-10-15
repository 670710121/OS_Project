import java.io.*;
import java.net.InetSocketAddress;
import java.nio.channels.FileChannel;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.nio.file.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Stream;

/**
 * Server: ส่งไฟล์ให้ Client ตามช่วง byte ที่ขอ
 *
 * วิธีรัน:  java Server io    (ส่งข้อมูลแบบ Traditional I/O)
 *           java Server nio   (ส่งข้อมูลแบบ NIO transferTo)
 * ไฟล์ที่ให้ดาวน์โหลดต้องวางไว้ในโฟลเดอร์ "files"
 *
 * Protocol (ข้อความส่งด้วย writeUTF / อ่านด้วย readUTF):
 *   LIST                       -> FILE <name> <size> (หลายบรรทัด) แล้วปิดด้วย END
 *   INFO <name>                -> SIZE <bytes>  หรือ  ERROR <code> <message>
 *   GET <name> <offset> <len>  -> OK <len> แล้วตามด้วย payload ดิบ <len> bytes
 *                                 หรือ ERROR <code> <message>
 * (ชื่อไฟล์ห้ามมีช่องว่าง)
 */
public class Server {
    static final int PORT = 5000;
    static final Path DIR = Paths.get("files");
    static boolean useNio = false;   // true = ใช้ NIO ตอนส่งข้อมูล

    public static void main(String[] args) throws IOException {
        useNio = args.length > 0 && args[0].equalsIgnoreCase("nio");

        // Thread Pool: มี thread ทำงานพร้อมกันได้สูงสุด 20 ตัว
        // (Client 10 workers = 10 connection ต่อ 1 การดาวน์โหลด)
        ExecutorService pool = Executors.newFixedThreadPool(20);

        // ใช้ ServerSocketChannel เพราะ NIO transferTo ต้องใช้ SocketChannel
        ServerSocketChannel server = ServerSocketChannel.open();
        server.bind(new InetSocketAddress(PORT));
        System.out.println("Server started on port " + PORT + " mode=" + (useNio ? "NIO" : "Traditional I/O"));

        while (true) {
            SocketChannel client = server.accept();      // รอ Client ต่อเข้ามา (1 connection)
            pool.submit(() -> handleClient(client));     // โยนให้ thread ใน pool ไปดูแล
        }
    }

    // thread ใน pool 1 ตัวดูแล 1 connection: อ่านคำสั่ง -> ตอบกลับ วนไปจนกว่า Client จะปิด
    static void handleClient(SocketChannel ch) {
        try (ch) {
            DataInputStream in = new DataInputStream(ch.socket().getInputStream());
            DataOutputStream out = new DataOutputStream(ch.socket().getOutputStream());

            while (true) {
                String line;
                try {
                    line = in.readUTF();                 // อ่านคำสั่งหนึ่งบรรทัด
                } catch (EOFException e) {
                    break;                               // Client ปิด connection แล้ว
                }
                String[] p = line.split(" ");

                if (p[0].equals("LIST")) {
                    doList(out);
                } else if (p[0].equals("INFO") && p.length == 2) {
                    doInfo(p[1], out);
                } else if (p[0].equals("GET") && p.length == 4) {
                    doGet(p[1], p[2], p[3], ch, out);
                } else {
                    out.writeUTF("ERROR 400 Bad request");
                }
            }
        } catch (IOException e) {
            System.out.println("Connection error: " + e.getMessage());
        }
    }

    // LIST: ส่งรายชื่อไฟล์ + ขนาด
    static void doList(DataOutputStream out) throws IOException {
        try (Stream<Path> files = Files.list(DIR)) {
            for (Path f : (Iterable<Path>) files.filter(Files::isRegularFile)::iterator) {
                out.writeUTF("FILE " + f.getFileName() + " " + Files.size(f));
            }
        }
        out.writeUTF("END");
    }

    // INFO: ส่งขนาดไฟล์
    static void doInfo(String name, DataOutputStream out) throws IOException {
        Path file = findFile(name);
        if (file == null) {
            out.writeUTF("ERROR 404 File not found");
        } else {
            out.writeUTF("SIZE " + Files.size(file));
        }
    }

    // GET: ส่งข้อมูลเฉพาะช่วง [offset, offset+length)
    static void doGet(String name, String offStr, String lenStr,
                      SocketChannel ch, DataOutputStream out) throws IOException {
        Path file = findFile(name);
        if (file == null) {
            out.writeUTF("ERROR 404 File not found");
            return;
        }

        long offset, length;
        try {
            offset = Long.parseLong(offStr);
            length = Long.parseLong(lenStr);
        } catch (NumberFormatException e) {
            out.writeUTF("ERROR 400 Offset/length must be numbers");
            return;
        }

        // ตรวจว่าช่วงที่ขอต้องอยู่ในไฟล์
        if (offset < 0 || length < 0 || offset + length > Files.size(file)) {
            out.writeUTF("ERROR 416 Invalid range");
            return;
        }

        out.writeUTF("OK " + length);    // บอก Client ก่อนว่าจะส่ง payload กี่ byte
        out.flush();

        if (useNio) {
            sendWithNio(file, offset, length, ch);
        } else {
            sendWithTraditionalIO(file, offset, length, out);
        }
    }

    // ---------- วิธีที่ 1: Traditional I/O ----------
    // อ่านไฟล์เข้า byte[] (หน่วยความจำของ Java) แล้วค่อยเขียนลง socket
    // ข้อมูลถูกก๊อปปี้หลายรอบ: disk -> kernel -> byte[] -> kernel(socket) -> network
    static void sendWithTraditionalIO(Path file, long offset, long length, OutputStream out)
            throws IOException {
        try (RandomAccessFile raf = new RandomAccessFile(file.toFile(), "r")) {
            raf.seek(offset);                                   // ข้ามไปที่ offset
            byte[] buf = new byte[64 * 1024];
            long left = length;                                 // เหลืออีกกี่ byte
            while (left > 0) {
                int n = raf.read(buf, 0, (int) Math.min(buf.length, left));
                if (n < 0) break;                               // จบไฟล์
                out.write(buf, 0, n);
                left -= n;
            }
            out.flush();
        }
    }

    // ---------- วิธีที่ 2: NIO transferTo ----------
    // สั่งให้ OS ส่งจากไฟล์ไป socket โดยตรง (zero-copy) ไม่ต้องผ่าน byte[] ใน Java
    static void sendWithNio(Path file, long offset, long length, SocketChannel ch)
            throws IOException {
        try (FileChannel fc = FileChannel.open(file, StandardOpenOption.READ)) {
            long pos = offset;
            long left = length;
            while (left > 0) {
                long n = fc.transferTo(pos, left, ch);          // อาจส่งได้น้อยกว่าที่ขอ จึงต้องวน
                if (n <= 0) break;
                pos += n;
                left -= n;
            }
        }
    }

    // หาไฟล์ในโฟลเดอร์ files (กันชื่อแปลก ๆ เช่น ../secret.txt)
    static Path findFile(String name) {
        if (!Paths.get(name).getFileName().toString().equals(name)) return null;
        Path f = DIR.resolve(name);
        return Files.isRegularFile(f) ? f : null;
    }
}
