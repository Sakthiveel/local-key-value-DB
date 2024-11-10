# Limitation
    Currenrly only works with linux and mac os as there are Posix based os's . I tested in ubuntu 22
# How to Run It

To run this project, follow these steps:
1. Clone the repository to your local machine.
2. Install all dependencies using `go mod tidy`
3. Check `main.go` to create an db instance and run  `go run .`
4. Run the test functions individually  `go test -run TestFuncName` Please check `db_test.go`
5. Run all test functions `go test .`

# Design

- **Concurrency Management**: I chose to use Go channels over RWMutex for managing concurrency. While RWMutex allows multiple concurrent reads, it can return outdated data if a write occurs simultaneously. By using Go channels, I ensure sequential execution of reads and writes, which guarantees data consistency and simplifies concurrency handling. To improve throughput, I can implement separate Go channels for reading and writing, allowing them to run in parallel. Synchronization between these channels can be maintained by ensuring reads pause when a write is in progress, using simple mechanisms like flags or state checks to prevent stale data access. This approach provides a balance between simplicity and efficiency, making it an ideal solution for maintaining data integrity.

- **Modular Architecture**: The project follows a modular approach, with the core logic and the database management separated into different components. This makes it easy to extend and maintain. The design is flexible enough to allow for easy integration of new storage mediums in the future, not just limited to local storage.

- **Type Safety with Generics**: The database layer leverages Go's generics to ensure type safety. By using the `DbData` type, which accepts values of any type, we can store a wide range of data structures as database values. This ensures that type errors are caught at compile time, and developers can work with a variety of data types without sacrificing safety.

# Journey

This project has evolved through several iterations:

- **Initial Development**: The main focus in the beginning was to build the basic functionalities, such as setting up the in-memory data structure and implementing essential CRD operations.
  
- **Testing for Robustness**: After completing the basic operations, I added concurrency testing and stress testing to ensure that the system could handle multiple requests efficiently and maintain stability under high load.


I am completely new to Go and primarily a JavaScript developer, so working on this project has been a great opportunity to leverage and learn Go’s unique features. It’s been a valuable and enriching experience, as I’ve learned a lot about Go’s concurrency model, type safety, and performance optimizations.




# Improvements

1. **Seperate Go Channel**: Currently I am using only one Go channel for processing every db operation. But we can extend  by creating seperate Go Channels for different operation type , of course will need to make sure about synchronization .Thus we can increase the overall system throughput.

2. **File Writing Optimization**: Currently, every operation modifies the in-memory data structure and overwrites the file with the updated content. A potential improvement is to switch to using a binary file format, which would allow appending new entries instead of overwriting the file every time. While this would help improve performance and reduce file I/O overhead, I am not yet familiar with binary file formats, so further research is required before implementing this change.

3. **Update Operation**: This can be implmented easily.
   

# Note
   
   It took 5 hours to complete the project to this level (still writing the readme section). Any kind of feedback is very appreciated . Thank you for taking time to look into this project.

