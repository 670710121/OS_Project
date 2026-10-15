package threadv2;

import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * จัดการ permit ของ Printer และ Database ที่ใช้ร่วมกัน
 * Worker ทุกตัวจะใช้ ResourceManager instance เดียวกัน
 */
public class ResourceManager {
    private final Semaphore printer;                 // จำกัดจำนวนงานที่ใช้ Printer พร้อมกัน
    private final Semaphore database;                // จำกัดจำนวนงานที่ใช้ Database พร้อมกัน
    private final int printerCapacity;               // จำนวน permit ของ Printer ตามค่าตั้งต้น ใช้แสดงสถานะ
    private final int databaseCapacity;              // จำนวน permit ของ Database ตามค่าตั้งต้น ใช้แสดงสถานะ
    private final AtomicInteger printersInUse = new AtomicInteger(); // ตัวนับการใช้งานปัจจุบันที่ Monitor อ่านได้อย่างปลอดภัย
    private final AtomicInteger databasesInUse = new AtomicInteger(); // ตัวนับการใช้งานปัจจุบันที่ Monitor อ่านได้อย่างปลอดภัย

    public ResourceManager(int printerPermits, int databasePermits) {
        printerCapacity = printerPermits;            // เก็บจำนวนความจุที่ตั้งค่าไว้เพื่อให้ Monitor ใช้รายงาน
        databaseCapacity = databasePermits;          // เก็บจำนวนความจุที่ตั้งค่าไว้เพื่อให้ Monitor ใช้รายงาน
        printer = new Semaphore(printerPermits, true); // Semaphore แบบ fair ช่วยจัดลำดับ Thread ที่กำลังรออย่างเป็นธรรม
        database = new Semaphore(databasePermits, true); // Semaphore แบบ fair ที่ Worker ทุกตัวใช้ร่วมกัน
    }

    public void acquire(ResourceType type) throws InterruptedException {
        if (type == ResourceType.PRINTER) {           // เลือก Semaphore ของ Printer เมื่อมีการร้องขอ
            printer.acquire();                        // รอจนกว่า permit ของ Printer จะว่างหนึ่งสิทธิ์
            printersInUse.incrementAndGet();          // บันทึกการได้รับสิทธิ์สำเร็จเพื่อให้ Monitor ใช้รายงาน
        } else if (type == ResourceType.DATABASE) {   // เลือก Semaphore ของ Database เมื่อมีการร้องขอ
            database.acquire();                       // รอจนกว่า permit ของ Database จะว่าง
            databasesInUse.incrementAndGet();         // บันทึกการได้รับสิทธิ์สำเร็จเพื่อให้ Monitor ใช้รายงาน
        }                                           // ค่า NONE ไม่ต้องใช้ permit
    }

    public void release(ResourceType type) {
        if (type == ResourceType.PRINTER) {           // คืนเฉพาะทรัพยากรที่งานนี้ได้รับสิทธิ์มา
            printersInUse.decrementAndGet();          // ปรับตัวนับการใช้งานก่อนคืน permit
            printer.release();                        // ทำให้ permit ของ Printer กลับมาว่างหนึ่งสิทธิ์
        } else if (type == ResourceType.DATABASE) {   // จัดการงานที่ใช้ Database ด้วยวิธีเดียวกัน
            databasesInUse.decrementAndGet();         // ปรับจำนวนการใช้งานที่แสดงให้เห็น
            database.release();                       // คืน permit ของ Database หนึ่งสิทธิ์
        }                                           // NONE ไม่มีทรัพยากรให้คืน
    }

    public String status() {
        return String.format("printer=%d/%d database=%d/%d",
                printersInUse.get(), printerCapacity, // แสดงจำนวน permit ของ Printer ที่ใช้อยู่ / จำนวนทั้งหมด
                databasesInUse.get(), databaseCapacity); // แสดงจำนวน permit ของ Database ที่ใช้อยู่ / จำนวนทั้งหมด
    }
}
