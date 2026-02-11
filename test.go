package main

import (
	"fmt"
	"math/rand"
	"sync"
	"time"
)

// WorkerPool demonstrates a pool of goroutines processing jobs
type WorkerPool struct {
	workerCount int
	jobs        chan int
	results     chan int
	wg          sync.WaitGroup
}

// NewWorkerPool creates a new WorkerPool
func NewWorkerPool(workerCount, jobCount int) *WorkerPool {
	return &WorkerPool{
		workerCount: workerCount,
		jobs:        make(chan int, jobCount),
		results:     make(chan int, jobCount),
	}
}

// Start initializes the pool and launches workers
func (wp *WorkerPool) Start() {
	for i := 0; i < wp.workerCount; i++ {
		wp.wg.Add(1)
		go wp.worker(i)
	}
}

// AddJob adds a job to the pool
func (wp *WorkerPool) AddJob(job int) {
	wp.jobs <- job
}

// CloseJobs closes the jobs channel
func (wp *WorkerPool) CloseJobs() {
	close(wp.jobs)
}

// Wait waits for all workers to finish
func (wp *WorkerPool) Wait() {
	wp.wg.Wait()
	close(wp.results)
}

// worker is the function run by each goroutine in the pool
func (wp *WorkerPool) worker(id int) {
	defer wp.wg.Done()
	for job := range wp.jobs {
		result := process(job)
		fmt.Printf("Worker %d processed job %d, result: %d\n", id, job, result)
		wp.results <- result
	}
}

// process simulates a time-consuming job
func process(n int) int {
	time.Sleep(time.Millisecond * time.Duration(rand.Intn(200)))
	return n * n
}

// fibonacci calculates the nth Fibonacci number recursively
func fibonacci(n int) int {
	if n <= 1 {
		return n
	}
	return fibonacci(n-1) + fibonacci(n-2)
}

// printFibonacci prints the first n Fibonacci numbers
func printFibonacci(n int) {
	fmt.Printf("Fibonacci sequence up to %d:\n", n)
	for i := 0; i < n; i++ {
		fmt.Printf("%d ", fibonacci(i))
	}
	fmt.Println()
}

func main() {
	rand.Seed(time.Now().UnixNano())
	fmt.Println("Starting WorkerPool example...")

	// WorkerPool example
	const jobCount = 10
	wp := NewWorkerPool(3, jobCount)
	wp.Start()

	for i := 1; i <= jobCount; i++ {
		wp.AddJob(i)
	}
	wp.CloseJobs()
	wp.Wait()

	sum := 0
	for result := range wp.results {
		sum += result
	}
	fmt.Printf("Sum of results: %d\n", sum)

	// Fibonacci example
	printFibonacci(10)

	// Simulated HTTP server example
	requests := make(chan string, 5)
	responses := make(chan string, 5)
	quit := make(chan struct{})

	//go simulateHTTPServer(requests, responses, quit)

	for i := 0; i < 5; i++ {
		requests <- fmt.Sprintf("request-%d", i)
	}

	for i := 0; i < 5; i++ {
		fmt.Println(<-responses)
	}

	close(quit)
	fmt.Println("All examples complete.")
}
