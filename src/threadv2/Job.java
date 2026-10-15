package threadv2;

/**
 * แทนงานหนึ่งรายการที่อ่านมาจากไฟล์ workload CSV
 * ข้อมูลต้นฉบับของงานเป็น final ส่วนเวลาที่วัดได้จะถูกบันทึกระหว่างการจำลอง
 */
public class Job {
    public final String id;                    // รหัสงานที่ไม่ซ้ำกันจาก CSV
    public final long arrivalMs;               // เวลาที่กำหนดให้งานมาถึง นับจากเริ่มการจำลอง
    public final int priority;                 // ตัวเลขน้อยกว่าหมายถึงมีความสำคัญสูงกว่า
    public final long workMs;                  // ระยะเวลาจำลองของงานหลัก
    public final ResourceType resource;        // ทรัพยากรร่วมที่ต้องใช้ หรือ NONE หากไม่ต้องใช้
    public final long resourceMs;               // ระยะเวลาจำลองที่ใช้ครอบครองทรัพยากร
    public final int sequence;                 // ลำดับเดิมใน CSV ใช้ตัดสินกรณี priority เท่ากันอย่างแน่นอน

    public volatile long actualArrivalMs = -1; // เวลาจริงที่ Generator ปล่อยงานนี้เข้าสู่ระบบ
    public volatile long startTimeMs = -1;     // เวลาจริงที่ Worker เริ่มทำงานนี้
    public volatile long completionTimeMs = -1;// เวลาจริงที่งานนี้เสร็จสมบูรณ์
    public volatile long resourceWaitMs = 0;   // เวลารวมที่ใช้รอรับ permit ของทรัพยากร

    public Job(String id, long arrivalMs, int priority, long workMs,
               ResourceType resource, long resourceMs, int sequence) {
        this.id = id;                           // บันทึกรหัสงาน
        this.arrivalMs = arrivalMs;             // บันทึกเวลามาถึงที่กำหนดไว้
        this.priority = priority;               // บันทึกค่าลำดับความสำคัญ
        this.workMs = workMs;                   // บันทึกระยะเวลาของงานหลัก
        this.resource = resource;                // บันทึกชนิดของทรัพยากร
        this.resourceMs = resourceMs;            // บันทึกระยะเวลาที่ครอบครองทรัพยากร
        this.sequence = sequence;                // บันทึกลำดับเดิมในข้อมูลนำเข้า
    }

    @Override
    public String toString() {
        return String.format("%s(priority=%d, work=%dms, resource=%s)",
                id, priority, workMs, resource); // แสดงรายละเอียดงานแบบสั้นที่อ่านเข้าใจง่าย
    }
}
