package threadv2;

/**
 * รายงานภาพรวมสถานะของคิว Worker งานที่เสร็จแล้ว และทรัพยากรเป็นระยะ
 */
public class Monitor extends Thread {
    private final ReadyQueue readyQueue;               // แหล่งข้อมูลจำนวนงานที่พร้อมทำ
    private final ResourceManager resources;           // แหล่งข้อมูลการใช้งานทรัพยากรร่วม
    private final Statistics statistics;               // แหล่งข้อมูลจำนวนงานที่กำลังทำและทำเสร็จแล้ว
    private final ProjectLogger logger;                // พิมพ์ภาพรวมสถานะตามรูปแบบ log มาตรฐาน

    public Monitor(ReadyQueue readyQueue, ResourceManager resources,
                   Statistics statistics, ProjectLogger logger) {
        super("monitor");                              // ตั้งชื่อ Thread ให้ชัดเจนใน log
        this.readyQueue = readyQueue;                   // เก็บอ้างอิงไปยัง Ready Queue
        this.resources = resources;                     // เก็บอ้างอิงไปยัง ResourceManager
        this.statistics = statistics;                   // เก็บอ้างอิงไปยัง Statistics ที่ใช้ร่วมกัน
        this.logger = logger;                           // เก็บอ้างอิงไปยัง Logger
    }

    @Override
    public void run() {
        try {
            while (true) {                             // รายงานต่อไปจนกว่า Main จะ interrupt Monitor
                logger.monitor(readyQueue.size(), statistics.runningCount(),
                        statistics.completedCount(), resources.status()); // อ่านและรายงานค่าปัจจุบัน
                Thread.sleep(1_000);                    // รอประมาณหนึ่งวินาทีระหว่างการรายงานแต่ละครั้ง
            }
        } catch (InterruptedException e) {
            // การ interrupt เป็นสัญญาณปิด Monitor ตามปกติ ไม่ต้องตั้งสถานะกลับแล้ววนลูปต่อ
        }
    }
}
