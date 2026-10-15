import java.util.PriorityQueue;
import java.util.Comparator;

/**
 * การจัดลำดับงานตาม Priority แบบไม่แย่งงานกลางคัน (non-preemptive)
 * Priority 1 มีความสำคัญสูงสุด หากเท่ากันให้ใช้ลำดับเดิมใน CSV ไม่ใช้จังหวะการทำงานของ Thread
 */
public class PriorityQueueImpl implements ReadyQueue {
    private final PriorityQueue<Job> jobs = new PriorityQueue<>(
            Comparator.comparingInt((Job job) -> job.priority) // ค่า priority ที่เป็นตัวเลขน้อยกว่าจะถูกเลือกก่อน
                      .thenComparingInt(job -> job.sequence)); // หาก priority เท่ากัน ให้ใช้ลำดับเดิมใน CSV
    private boolean shutDown = false;                          // ระบุว่าจะไม่มีงานใหม่เข้ามาอีกแล้วหรือไม่

    @Override
    public synchronized void add(Job job) {
        if (shutDown) {                                        // ปฏิเสธงานใหม่เมื่อเริ่มปิดระบบแล้ว
            throw new IllegalStateException("Cannot add a job after queue shutdown");
        }
        jobs.add(job);                                         // เพิ่มงานตามลำดับ priority
        notifyAll();                                           // ปลุก Worker ที่กำลังรอรับงาน
    }

    @Override
    public synchronized Job take() throws InterruptedException {
        while (jobs.isEmpty() && !shutDown) {                   // รอหากยังไม่มีงานที่พร้อมทำ
            wait();                                            // รออย่างมีประสิทธิภาพแทนการวนลูปตรวจซ้ำ
        }
        if (jobs.isEmpty()) {                                  // ระบบกำลังปิดและไม่มีงานเหลือ
            return null;                                       // แจ้งให้ Worker หยุดทำงาน
        }
        return jobs.remove();                                  // คืนงานที่มี priority สูงที่สุดในบรรดางานที่พร้อมทำ
    }

    @Override
    public synchronized int size() {
        return jobs.size();                                    // รายงานจำนวนงานที่รออยู่อย่างปลอดภัย
    }

    @Override
    public synchronized void shutdown() {
        shutDown = true;                                       // ป้องกันไม่ให้เพิ่มงานใหม่อีก
        notifyAll();                                           // ปลุก Worker ที่รออยู่ให้ตรวจสอบสถานะปิดระบบ
    }
}
