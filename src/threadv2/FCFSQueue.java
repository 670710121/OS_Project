import java.util.ArrayDeque;
import java.util.Queue;

/**
 * คิวพร้อมทำงานแบบมาก่อนได้ก่อน (FCFS)
 * งานที่ถูกเพิ่มเข้าคิวก่อน จะถูกนำออกไปทำก่อน
 */
public class FCFSQueue implements ReadyQueue {
    private final Queue<Job> jobs = new ArrayDeque<>(); // เก็บงานตามลำดับที่เข้ามาใน Ready Queue
    private boolean shutDown = false;                   // ระบุว่าระบบปิดรับงานใหม่แล้วหรือยัง

    @Override
    public synchronized void add(Job job) {
        if (shutDown) {                                 // ปฏิเสธงานใหม่เมื่อเริ่มปิดระบบแล้ว
            throw new IllegalStateException("Cannot add a job after queue shutdown");
        }
        jobs.add(job);                                  // เพิ่มงานไว้ท้ายคิว
        notifyAll();                                    // ปลุก Worker ที่กำลังรอรับงาน
    }

    @Override
    public synchronized Job take() throws InterruptedException {
        while (jobs.isEmpty() && !shutDown) {            // รอเมื่อยังไม่มีงานและระบบยังทำงานอยู่
            wait();                                     // ปล่อย lock แล้วรอ โดยไม่วนลูปใช้ CPU เปลือง (busy waiting)
        }
        if (jobs.isEmpty()) {                            // ไม่มีงานเหลือและมีคำสั่งปิดระบบแล้ว
            return null;                                // คืนค่า null เพื่อบอกให้ Worker จบการทำงาน
        }
        return jobs.remove();                            // นำงานที่อยู่ในคิวนานที่สุดออกมา
    }

    @Override
    public synchronized int size() {
        return jobs.size();                              // อ่านจำนวนงานขณะถือ lock เดียวกัน
    }

    @Override
    public synchronized void shutdown() {
        shutDown = true;                                 // ระบุว่าคิวปิดรับงานใหม่แล้ว
        notifyAll();                                    // ปลุก Worker เพื่อให้ตรวจสอบและออกเมื่อคิวว่าง
    }
}
