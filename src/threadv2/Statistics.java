import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * เก็บค่าการวัดประสิทธิภาพของการจำลองหนึ่งรอบ โดยรองรับการใช้งานหลาย Thread อย่างปลอดภัย
 * Worker หลายตัวอาจทำงานเสร็จพร้อมกัน จึงต้องใช้ synchronized ป้องกันการแก้ไขข้อมูลร่วมกัน
 */
public class Statistics {
    private final List<Job> completedJobs = new ArrayList<>(); // เก็บงานที่ทำเสร็จแล้ว
    private final AtomicInteger running = new AtomicInteger(); // นับจำนวนงานที่กำลังประมวลผลอยู่

    public void jobStarted() {
        running.incrementAndGet();                       // เพิ่มจำนวนงานที่กำลังทำอย่างปลอดภัย
    }

    public void jobStopped() {
        running.decrementAndGet();                       // ลดจำนวนงานที่กำลังทำอย่างปลอดภัย
    }

    public int runningCount() {
        return running.get();                             // คืนค่าภาพรวมจำนวนงานที่กำลังทำอย่างปลอดภัย
    }

    public synchronized void recordCompletion(Job job) {
        completedJobs.add(job);                           // เก็บงานที่เสร็จแล้วขณะถือ lock ของ object
        notifyAll();                                     // แจ้ง Controller ที่กำลังรอการอัปเดตสถานะงานเสร็จ
    }

    public synchronized int completedCount() {
        return completedJobs.size();                      // อ่านจำนวนขณะถือ lock เดียวกัน
    }

    public synchronized void printSummary(List<Job> allJobs, long makespanMs) {
        double waitingTotal = 0;                          // สะสมค่า Waiting Time ของแต่ละงาน
        double turnaroundTotal = 0;                       // สะสมค่า Turnaround Time ของแต่ละงาน
        double resourceWaitTotal = 0;                     // สะสมเวลารอเฉพาะงานที่ต้องใช้ทรัพยากร
        int resourceJobCount = 0;                          // นับจำนวนงานที่นำมาคำนวณค่าเฉลี่ย Resource Wait Time

        for (Job job : allJobs) {                         // คำนวณตัวชี้วัดของงานทุกงานใน workload
            long actualArrival = Math.max(0, job.actualArrivalMs); // ใช้เวลามาถึงจริง ไม่ใช่เวลาที่วางแผนไว้
            long waiting = Math.max(0, job.startTimeMs - actualArrival); // เวลาเริ่มงานลบด้วยเวลามาถึงจริง
            long turnaround = Math.max(0, job.completionTimeMs - actualArrival); // เวลาเสร็จงานลบด้วยเวลามาถึงจริง
            waitingTotal += waiting;                      // เพิ่ม Waiting Time ของงานนี้
            turnaroundTotal += turnaround;                // เพิ่ม Turnaround Time ของงานนี้
            if (job.resource != ResourceType.NONE) {      // ไม่นับงานที่ไม่ได้ร้องขอทรัพยากรร่วม
                resourceWaitTotal += job.resourceWaitMs;  // เพิ่มเวลาที่วัดได้จากการรอ Semaphore
                resourceJobCount++;                       // นับงานที่ใช้ทรัพยากรนี้ในการคำนวณค่าเฉลี่ย
            }
        }

        double avgWaiting = allJobs.isEmpty() ? 0 : waitingTotal / allJobs.size(); // ป้องกันการหารด้วยศูนย์
        double avgTurnaround = allJobs.isEmpty() ? 0 : turnaroundTotal / allJobs.size(); // คำนวณค่าเฉลี่ยจากงานทั้งหมด
        double avgResourceWait = resourceJobCount == 0 ? 0 : resourceWaitTotal / resourceJobCount; // คำนวณค่าเฉลี่ยเฉพาะงานที่ใช้ทรัพยากร
        double throughput = makespanMs <= 0 ? 0 : completedJobs.size() / (makespanMs / 1000.0); // จำนวนงานที่เสร็จต่อวินาที

        System.out.println();                             // เว้นบรรทัดแยกส่วนสรุปออกจาก log ดิบ
        System.out.println("========== SIMULATION SUMMARY =========="); // หัวข้อส่วนสรุปผล
        System.out.printf("Completed jobs: %d/%d%n", completedJobs.size(), allJobs.size()); // จำนวนงานที่เสร็จแล้ว
        System.out.printf("Average Waiting Time: %.0f ms%n", avgWaiting); // ตัวชี้วัดที่ต้องแสดงเป็นจำนวนเต็มหน่วยมิลลิวินาที
        System.out.printf("Average Turnaround Time: %.0f ms%n", avgTurnaround); // ตัวชี้วัดที่ต้องแสดงเป็นจำนวนเต็มหน่วยมิลลิวินาที
        System.out.printf("Average Resource Wait Time: %.0f ms%n", avgResourceWait); // ค่าเฉลี่ยเฉพาะงานที่ใช้ทรัพยากร
        System.out.printf("Throughput: %.2f jobs/second%n", throughput); // แสดงทศนิยมอย่างน้อยสองตำแหน่งตามที่กำหนด
        System.out.printf("Makespan: %d ms%n", makespanMs); // ระยะเวลาจำลองรวมที่วัดได้
        System.out.println("========================================"); // ปิดส่วนสรุปผล
    }
}
