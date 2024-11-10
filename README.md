# How to Run It

To run this project, follow these steps:
1. Clone the repository to your local machine.
2. Install all dependencies using `go mod tidy`.
3. Run the main Go file using `go run main.go`.
4. Ensure that your database (if required) is set up before running the application.

# Design

The system is designed with a focus on scalability, modularity, and type safety. It efficiently handles concurrent operations while maintaining strict control over execution order, leveraging Go's goroutines and channels.

- **Concurrency Management**: While the application supports concurrent requests, it processes them sequentially by using Go channels. This ensures that operations are executed in order, as the order of execution between goroutines is not guaranteed. By using channels, we can manage concurrency while still enforcing a predictable and sequential flow of operations.

- **Modular Architecture**: The project follows a modular approach, with the core logic and the database management separated into different components. This makes it easy to extend and maintain. The design is flexible enough to allow for easy integration of new storage mediums in the future, not just limited to local storage.

- **Type Safety with Generics**: The database layer leverages Go's generics to ensure type safety. By using the `DbData` type, which accepts values of any type, we can store a wide range of data structures as database values. This ensures that type errors are caught at compile time, and developers can work with a variety of data types without sacrificing safety.


# Journey

This project evolved through several iterations:
- Initially, the focus was on building the basic functionalities.
- After that, concurrency testing and stress testing were added to ensure robustness.
- Finally, optimization for scalability was implemented to handle more significant amounts of data.

# Journey

This project has evolved through several iterations:

- **Initial Development**: The main focus in the beginning was to build the basic functionalities, such as setting up the in-memory data structure and implementing essential CRUD operations.
  
- **Testing for Robustness**: After completing the basic operations, I added concurrency testing and stress testing to ensure that the system could handle multiple requests efficiently and maintain stability under high load.


I am completely new to Go and primarily a JavaScript developer, so working on this project has been a great opportunity to leverage and learn Go’s unique features. It’s been a valuable and enriching experience, as I’ve learned a lot about Go’s concurrency model, type safety, and performance optimizations.

### Known Improvements

1. **File Writing Optimization**: Currently, every operation modifies the in-memory data structure and overwrites the file with the updated content. A potential improvement is to switch to using a binary file format, which would allow appending new entries instead of overwriting the file every time. While this would help improve performance and reduce file I/O overhead, I am not yet familiar with binary file formats, so further research is required before implementing this change.


# Improvements

Some future improvements include:
- Adding detailed logging to track performance issues.
- Implementing more granular error handling for edge cases.
- Optimizing database queries for better performance under high load.
- Exploring advanced caching mechanisms to reduce database hits.

# Resume Link

[Click here to view my resume](#)
