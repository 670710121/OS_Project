package threadv2;

import java.util.concurrent.BlockingQueue;

/**
 * Thread จริงที่ทำหน้าที่อยู่ระหว่าง JobGenerator กับ Ready Queue ที่เลือกใช้
 * เป็นส่วนประกอบเดียวที่ย้ายงานที่มาถึงแล้วเข้าสู่นโยบายจัดลำดับงาน
 */
public class Scheduler extends Thread {
    private final BlockingQueue<Job> arrivalQueue; // รับงานจาก JobGenerator
    private final ReadyQueue readyQueue;            // ส่งงานไปยังคิว FCFS หรือ Priority
    private final ProjectLogger logger;             // บันทึกเหตุการณ์ของ Scheduler

    public Scheduler(BlockingQueue<Job> arrivalQueue, ReadyQueue readyQueue, ProjectLogger logger) {
        super("scheduler");                         // ตั้งชื่อ Thread ให้ชัดเจนใน log
        this.arrivalQueue = arrivalQueue;            // เก็บคิวที่ใช้ส่งต่องานเมื่อมาถึง
        this.readyQueue = readyQueue;                // เก็บนโยบายจัดลำดับงานที่เลือกใช้
        this.logger = logger;                        // เก็บอ้างอิงไปยัง Logger ที่ใช้ร่วมกัน
    }

    @Override
    public void run() {
        try {
            while (true) {                          // รับงานที่มาถึงต่อไปจนกว่าจะพบตัวบอกให้หยุด
                Job job = arrivalQueue.take();       // รออย่างมีประสิทธิภาพจนกว่า Generator จะส่งงานมา
                if (job == JobGenerator.STOP_JOB) {  // ตรวจสอบว่า Generator ปล่อยงานครบแล้วหรือยัง
                    break;                          // หยุดจัดลำดับเมื่อรับงานจริงครบทุกงานแล้ว
                }
                readyQueue.add(job);                 // เพิ่มงานลง Ready Queue ของนโยบายที่เลือก
                logger.systemEvent(job.id + " READY"); // บันทึกว่างานเข้าสู่สถานะพร้อมทำแล้ว
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();      // ตั้งสถานะ interrupt กลับ หาก Scheduler ถูก interrupt
        } finally {
            readyQueue.shutdown();                   // อนุญาตให้ Worker ออกจากระบบหลังรับงานในคิวไปครบแล้ว
        }
    }
}
