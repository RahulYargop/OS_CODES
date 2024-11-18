import java.util.LinkedList;
import java.util.Queue;
import java.util.concurrent.Semaphore;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

class ProducerConsumer {
    
    // Buffer size and item production limit
    private static final int BUFFER_SIZE = 7;
    private static final int MAX_ITEMS = 15;  // Limit for total items to be produced and consumed
    
    // Shared buffer
    private Queue<Integer> buffer = new LinkedList<>();
    
    // Semaphores to track the buffer's state
    private Semaphore empty = new Semaphore(BUFFER_SIZE);  // Initially, all buffer slots are empty
    private Semaphore full = new Semaphore(0);             // Initially, no buffer slots are full
    
    // Mutex (Lock) to control access to the buffer
    private Lock mutex = new ReentrantLock();
    
    // Shared counter to track the number of items produced and consumed
    private int producedItems = 0;
    private int consumedItems = 0;
    
    // Producer class
    class Producer extends Thread {
        @Override
        public void run() {
            try {
                while (true) {
                    mutex.lock();
                    if (producedItems >= MAX_ITEMS) {
                        mutex.unlock();
                        break;  // Stop production when MAX_ITEMS is reached
                    }
                    mutex.unlock();
                    
                    int item = produceItem();  // Produce an item
                    empty.acquire();  // Wait for an empty slot
                    mutex.lock();     // Acquire the lock to modify the buffer
                    
                    // Add the item to the buffer
                    buffer.add(item);
                    producedItems++;
                    System.out.println("Producer produced: " + item + " (Total produced: " + producedItems + ")");
                    
                    mutex.unlock();   // Release the lock
                    full.release();   // Signal that the buffer now has a full slot
                    
                    // Simulate production time
                    Thread.sleep((int) (Math.random() * 1000));
                }
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        
        private int produceItem() {
            return (int) (Math.random() * 100);  // Randomly produce an item
        }
    }
    
    // Consumer class
    class Consumer extends Thread {
        @Override
        public void run() {
            try {
                while (true) {
                    mutex.lock();
                    if (consumedItems >= MAX_ITEMS) {
                        mutex.unlock();
                        break;  // Stop consumption when MAX_ITEMS is reached
                    }
                    mutex.unlock();
                    
                    full.acquire();  // Wait for a full slot
                    mutex.lock();    // Acquire the lock to modify the buffer
                    
                    // Remove an item from the buffer
                    int item = buffer.remove();
                    consumedItems++;
                    System.out.println("Consumer consumed: " + item + " (Total consumed: " + consumedItems + ")");
                    
                    mutex.unlock();  // Release the lock
                    empty.release(); // Signal that the buffer now has an empty slot
                    
                    // Simulate consumption time
                    Thread.sleep((int) (Math.random() * 1500));
                }
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }
    
    public static void main(String[] args) {
        ProducerConsumer pc = new ProducerConsumer();
        
        // Create and start Producer and Consumer threads
        Producer producer = pc.new Producer();
        Consumer consumer = pc.new Consumer();
        
        producer.start();
        consumer.start();
        
        // Optionally, join threads (if you want the main thread to wait for them)
        try {
            producer.join();
            consumer.join();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
}
