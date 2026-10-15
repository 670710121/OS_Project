package threadv2;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * จุดเริ่มต้นของโปรแกรม: ตรวจสอบ input สร้างส่วนประกอบ และเริ่ม Thread ต่าง ๆ
 * จากนั้นรอให้งานทั้งหมดเสร็จ แล้วจึงแสดงผลการวัดประสิทธิภาพ
 */
public class Main {
    public static void main(String[] args) {
        Config config;                                 // เก็บค่าทั้งห้าที่รับมาจาก command line
        try {
            config = Config.parse(args);                // ตรวจสอบ argument ทั้งหมดก่อนเริ่ม Thread ใด ๆ
        } catch (IllegalArgumentException e) {
            System.err.println("Error: " + e.getMessage()); // อธิบายสาเหตุที่คำสั่งไม่ถูกต้อง
            System.err.println(Config.USAGE);            // แสดงรูปแบบคำสั่งที่ถูกต้อง
            return;                                     // จบโปรแกรมโดยยังไม่เริ่มการจำลอง
        }

        List<Job> jobs;                                 // เก็บ workload ที่อ่านจาก CSV
        try {
            jobs = WorkloadLoader.load(config.workloadPath); // ใช้ Loader ที่ให้มา ไม่เขียนตัวอ่าน CSV เอง
        } catch (WorkloadFormatException e) {
            System.err.println("Invalid workload: " + e.getMessage()); // อธิบายข้อผิดพลาดในการตรวจสอบ CSV
            return;                                     // หยุดก่อนเริ่ม Thread ใด ๆ
        } catch (IOException e) {
            System.err.println("Cannot open workload file: " + config.workloadPath); // แสดงพาธของไฟล์ที่ระบุ
            System.err.println("Check the path relative to the directory where Java was started."); // แสดงคำแนะนำที่ช่วยแก้ปัญหา
            return;                                     // หยุดก่อนเริ่ม Thread ใด ๆ
        }

        ProjectLogger logger = new ProjectLogger();      // เริ่มนาฬิกาจำลองและ Logger กลางเพียงชุดเดียว
        logger.systemStart(config);                     // บันทึกค่าที่ใช้รันครั้งนี้อย่างชัดเจนลง log
        logger.systemEvent("Loaded " + jobs.size() + " jobs"); // บันทึกจำนวนงานที่จะปล่อยเข้าสู่ระบบ

        ResourceManager resources = new ResourceManager(
                config.printerPermits, config.databasePermits); // สร้าง Semaphore แบบ fair ที่ใช้ร่วมกัน
        ReadyQueue readyQueue;                          // ประกาศตัวแปร Ready Queue ที่ไม่ผูกกับนโยบายใดโดยเฉพาะ
        if (config.policy == Config.Policy.FCFS) {
            readyQueue = new FCFSQueue();                // เลือกการจัดลำดับแบบมาก่อนได้ก่อน
        } else {
            readyQueue = new PriorityQueueImpl();        // เลือกตาม priority และใช้ลำดับ CSV เมื่อลำดับความสำคัญเท่ากัน
        }

        Statistics statistics = new Statistics();       // สร้างตัวนับร่วมและตัวเก็บค่าการวัด
        BlockingQueue<Job> arrivalQueue = new LinkedBlockingQueue<>(); // เชื่อม Generator กับ Scheduler ผ่านคิวที่ปลอดภัย

        Scheduler scheduler = new Scheduler(arrivalQueue, readyQueue, logger); // สร้าง Scheduler Thread
        JobGenerator generator = new JobGenerator(jobs, arrivalQueue, logger); // สร้างตัวปล่อยงานตามเวลามาถึง
        List<Worker> workers = new ArrayList<>();        // เก็บอ้างอิง Worker เพื่อให้ Main เรียก join() รอได้
        for (int i = 1; i <= config.workers; i++) {      // สร้าง Worker ตามจำนวนที่ผู้ใช้ระบุ
            workers.add(new Worker("worker-" + i, readyQueue, resources, statistics, logger)); // ให้ทุก Worker ใช้ตัวจัดการชุดเดียวกัน
        }
        Monitor monitor = new Monitor(readyQueue, resources, statistics, logger); // สร้าง Thread ที่รายงานสถานะเป็นระยะ

        scheduler.start();                              // เริ่ม Thread ฝั่งรับงานก่อนงานจะเริ่มมาถึง
        for (Worker worker : workers) {
            worker.start();                             // เริ่ม Worker ทุกตัวเพื่อให้รอรับงานจาก Ready Queue
        }
        monitor.start();                                // เริ่ม Monitor ก่อนเริ่มปล่อย workload
        generator.start();                              // เริ่มปล่อยงานตามเวลามาถึงที่กำหนด

        try {
            generator.join();                            // รอจนส่งงานทุกงานให้ Scheduler ครบ
            scheduler.join();                            // รอจน Scheduler ส่งงานทุกงานต่อและปิดคิว
            for (Worker worker : workers) {
                worker.join();                           // รอจน Worker ทุกตัวทำงานที่เหลือเสร็จ
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();          // คงสถานะ interrupt ของ Main ไว้
            logger.systemEvent("Main interrupted while waiting for shutdown"); // แสดงให้ทราบว่าเกิดการ interrupt
        } finally {
            monitor.interrupt();                         // หยุด Monitor ที่รายงานเป็นระยะเมื่อช่วงทำงานสิ้นสุด
            try {
                monitor.join();                          // ยืนยันว่า Monitor หยุดทำงานแล้วจริง ๆ
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();      // คงสถานะ interrupt หากการรอ Monitor ถูก interrupt
            }
        }

        long makespanMs = jobs.stream().mapToLong(job -> job.completionTimeMs).max().orElse(logger.now()); // ใช้เวลาที่งานสุดท้ายเสร็จเป็น makespan
        statistics.printSummary(jobs, makespanMs);      // แสดงค่าเฉลี่ยและ Throughput ตามที่กำหนด
        logger.systemStop(statistics.completedCount(), jobs.size()); // บันทึกจำนวนงานที่เสร็จสิ้นทั้งหมด
    }
}
