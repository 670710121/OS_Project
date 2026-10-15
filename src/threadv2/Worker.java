package threadv2;

/**
 * Worker จะรับงานที่พร้อมทำไปประมวลผลทีละงานจนเสร็จ
 * ลำดับการทำงานตาม PDF คือ ทำงานหลักก่อน แล้วจึงใช้ทรัพยากรร่วมถ้ามี
 */
public class Worker extends Thread {
    private final ReadyQueue readyQueue;             // คิวที่ Worker ทุกตัวใช้ร่วมกัน
    private final ResourceManager resources;         // ตัวจัดการ Semaphore ที่ Worker ทุกตัวใช้ร่วมกัน
    private final Statistics statistics;             // ตัวนับและค่าการวัดที่ใช้ร่วมกันและรองรับหลาย Thread อย่างปลอดภัย
    private final ProjectLogger logger;              // นาฬิกาและ Logger ที่ใช้ร่วมกัน

    public Worker(String name, ReadyQueue readyQueue, ResourceManager resources,
                  Statistics statistics, ProjectLogger logger) {
        super(name);                                 // ตั้งชื่อที่เข้าใจง่าย เช่น worker-1
        this.readyQueue = readyQueue;                 // เก็บอ้างอิงไปยัง Ready Queue
        this.resources = resources;                   // เก็บอ้างอิงไปยังทรัพยากรร่วม
        this.statistics = statistics;                 // เก็บอ้างอิงไปยัง Statistics ที่ใช้ร่วมกัน
        this.logger = logger;                         // เก็บอ้างอิงไปยัง Logger ที่ใช้ร่วมกัน
    }

    @Override
    public void run() {
        try {
            while (true) {                           // ทำงานต่อไปจนกว่าคิวจะแจ้งให้ปิดระบบ
                Job job = readyQueue.take();          // รอรับงานถัดไปโดยไม่ใช้ busy waiting
                if (job == null) {                    // ค่า null หมายถึงมีคำสั่งปิดระบบและไม่มีงานเหลือแล้ว
                    return;                           // จบการทำงานของ Worker อย่างเรียบร้อย
                }
                statistics.jobStarted();              // เพิ่มตัวนับจำนวนงานที่กำลังทำอย่างปลอดภัย
                try {
                    processJob(job);                  // ดำเนินขั้นตอนการทำงานของ Job ให้ครบ
                } finally {
                    statistics.jobStopped();          // ลดตัวนับงานที่กำลังทำ แม้การประมวลผลจะเกิดข้อผิดพลาด
                }
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();      // คงสถานะ interrupt และอนุญาตให้ Worker ออกจากการทำงาน
        }
    }

    private void processJob(Job job) {
        job.startTimeMs = logger.now();                // บันทึกเวลาจริงที่ Worker เริ่มทำงาน
        logger.jobStarted(job);                        // บันทึกเหตุการณ์เริ่มงานลง log

        sleepFor(job.workMs, "main work");             // จำลองการทำงานหลักก่อนขอใช้ทรัพยากร
        logger.workFinished(job);                      // บันทึกว่าขั้นตอนการทำงานหลักเสร็จแล้ว

        boolean acquired = false;                      // ตรวจสอบว่า Job นี้ได้รับ permit ของทรัพยากรแล้วหรือไม่
        if (job.resource != ResourceType.NONE) {       // งานที่ไม่ใช้ทรัพยากรจะข้ามขั้นตอน Semaphore
            long waitStarted = logger.now();            // บันทึกเวลาเริ่มรอทรัพยากร
            logger.resourceWaitStarted(job);            // บันทึกเหตุการณ์เริ่มรอทรัพยากรลง log
            acquireUntilSuccessful(job.resource);       // ลองขอใหม่เมื่อถูก interrupt เพื่อไม่ทิ้งงานค้างกลางทาง
            acquired = true;                           // ได้รับ permit สำเร็จแล้ว
            job.resourceWaitMs = logger.now() - waitStarted; // วัดเวลาจริงที่ใช้รอทรัพยากร
            logger.resourceAcquired(job, job.resourceWaitMs); // บันทึกการได้รับทรัพยากรสำเร็จและเวลาที่ใช้รอ
        }

        try {
            if (job.resource != ResourceType.NONE) {   // มีเฉพาะงานที่ใช้ทรัพยากรเท่านั้นที่ต้องทำขั้นตอนนี้
                sleepFor(job.resourceMs, "resource use"); // จำลองช่วงเวลาที่ครอบครองทรัพยากร
            }
        } finally {
            if (acquired) {                            // คืน permit เฉพาะเมื่อ Job นี้ได้รับ permit สำเร็จแล้ว
                resources.release(job.resource);       // คืน permit เสมอ แม้เกิดข้อผิดพลาดขณะทำงาน
                logger.resourceReleased(job);           // บันทึกเหตุการณ์คืนทรัพยากรลง log
            }
        }

        job.completionTimeMs = logger.now();            // บันทึกเวลาที่งานเสร็จจริง
        logger.jobCompleted(job);                       // บันทึกเหตุการณ์งานเสร็จลง log
        statistics.recordCompletion(job);               // บันทึกค่าการวัดของงานนี้อย่างปลอดภัย
    }

    private void acquireUntilSuccessful(ResourceType type) {
        while (true) {                                  // ลองขอ permit ต่อไปจนกว่างานจะได้รับสิทธิ์ที่ต้องการ
            try {
                resources.acquire(type);                // รออย่างเป็นธรรมจนกว่า permit จะว่าง
                return;                                 // เมื่อขอ permit สำเร็จ ก็ออกจากลูปได้
            } catch (InterruptedException e) {
                logger.systemEvent(getName() + " interrupted while waiting for " + type
                        + "; continuing job safely");   // อธิบายว่าเหตุใด Worker จึงไม่ทิ้งงานนี้
                // InterruptedException จะล้างสถานะ interrupt ทำให้การขอ permit ครั้งถัดไปรอได้ตามปกติ
            }
        }
    }

    private void sleepFor(long durationMs, String phase) {
        long endNanos = System.nanoTime() + durationMs * 1_000_000L; // ใช้นาฬิกา monotonic เพื่อวัดเวลาที่ผ่านไป
        while (true) {                                  // ทำซ้ำหาก sleep ถูก interrupt ก่อนครบเวลาที่กำหนด
            long remainingNanos = endNanos - System.nanoTime(); // คำนวณระยะเวลาจำลองที่ยังเหลือ
            if (remainingNanos <= 0) {                  // เวลาที่กำหนดผ่านไปครบแล้ว
                return;                                 // จบขั้นตอนนี้
            }
            long millis = remainingNanos / 1_000_000L;  // แปลงเวลาที่เหลือเป็นมิลลิวินาที
            int nanos = (int) (remainingNanos % 1_000_000L); // เก็บเศษเวลาที่น้อยกว่าหนึ่งมิลลิวินาทีไว้สำหรับ sleep
            try {
                Thread.sleep(millis, nanos);             // ใช้ sleep เพื่อรอเวลาที่เหลือโดยไม่วนลูปใช้ CPU
            } catch (InterruptedException e) {
                logger.systemEvent(getName() + " interrupted during " + phase
                        + "; finishing phase safely");  // บันทึกการถูก interrupt โดยไม่ทำให้งานหรือ permit สูญหาย
                // รอให้ครบเวลาที่เหลือ โดย InterruptedException จะล้างสถานะ interrupt
            }
        }
    }
}
