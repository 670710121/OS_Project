import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.BlockingQueue;

/**
 * ปล่อยงานลง Arrival Queue ตามเวลาที่กำหนดไว้ใน workload
 * คลาสนี้จะไม่เพิ่มงานลง Ready Queue โดยตรง เพราะเป็นหน้าที่ของ Scheduler
 */
public class JobGenerator extends Thread {
    static final Job STOP_JOB = new Job("__STOP__", 0, Integer.MAX_VALUE, 0,
            ResourceType.NONE, 0, Integer.MAX_VALUE); // ตัวบอกพิเศษนี้ไม่ถือเป็นงานจริง

    private final List<Job> jobs;                     // รายการงานที่จะปล่อยเข้าสู่ระบบ
    private final BlockingQueue<Job> arrivalQueue;    // คิวส่งต่องานระหว่าง Generator กับ Scheduler ที่รองรับหลาย Thread อย่างปลอดภัย
    private final ProjectLogger logger;               // Logger และนาฬิกาจำลองที่ใช้ร่วมกัน

    public JobGenerator(List<Job> jobs, BlockingQueue<Job> arrivalQueue, ProjectLogger logger) {
        super("generator");                            // ตั้งชื่อ Thread ให้ระบุได้ง่ายใน log
        this.jobs = new ArrayList<>(jobs);             // คัดลอกรายการก่อนเรียง เพื่อไม่เปลี่ยนลิสต์ที่ Loader ส่งมา
        this.jobs.sort(Comparator.comparingLong((Job job) -> job.arrivalMs)
                                 .thenComparingInt(job -> job.sequence)); // ปล่อยงานตามเวลาที่มาถึง และใช้ลำดับใน CSV เมื่อต้องตัดสินลำดับ
        this.arrivalQueue = arrivalQueue;              // เก็บคิวที่ใช้ส่งงานไปยัง Scheduler
        this.logger = logger;                           // เก็บ logger และนาฬิกากลางที่ใช้ร่วมกัน
    }

    @Override
    public void run() {
        try {
            for (Job job : jobs) {                     // ประมวลผลแต่ละรายการใน workload เพียงครั้งเดียว
                long waitMs = job.arrivalMs - logger.now(); // คำนวณเวลาที่เหลือก่อนงานนี้จะมาถึง
                if (waitMs > 0) {                      // หยุดรอเฉพาะเมื่อเวลามาถึงที่กำหนดยังอยู่ในอนาคต
                    Thread.sleep(waitMs);              // รอโดยไม่วนลูปใช้ CPU เปลือง
                }
                job.actualArrivalMs = logger.now();    // บันทึกเวลามาถึงจริงโดยใช้นาฬิกาของ Logger
                logger.jobArrived(job);                // บันทึกเหตุการณ์ที่งานมาถึงลงใน log
                arrivalQueue.put(job);                 // ส่งงานให้ Scheduler ผ่าน BlockingQueue
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();        // คงสถานะ interrupt ไว้เพื่อให้ปิดระบบได้อย่างเหมาะสม
        } finally {
            try {
                arrivalQueue.put(STOP_JOB);            // แจ้ง Scheduler ว่าจะไม่มีงานใหม่เข้ามาอีก
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();    // คงสถานะ interrupt ไว้ หากการส่งสัญญาณปิดระบบถูก interrupt
            }
        }
    }
}
