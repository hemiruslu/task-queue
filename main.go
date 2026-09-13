package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"sync"
	"syscall"
	"time"
)

type Job struct {
	ID      int
	Payload string
}

type JobStore struct {
	mu       sync.Mutex
	statuses map[int]string
	nextID   int
}

func (js *JobStore) AddJob(payload string) (int, string) {
	js.mu.Lock()
	defer js.mu.Unlock()
	id := js.nextID
	js.nextID++
	js.statuses[id] = "Pending"
	return id, "Pending"
}

func (js *JobStore) SetStatus(id int, status string) {
	js.mu.Lock()
	defer js.mu.Unlock()
	js.statuses[id] = status
}

func (js *JobStore) GetStatus(id int) (string, bool) {
	js.mu.Lock()
	defer js.mu.Unlock()
	status, exists := js.statuses[id]
	return status, exists
}

// İşçilere ne zaman mesaiyi bitireceklerini bilmeleri için WaitGroup ekledik
func worker(id int, jobs <-chan Job, store *JobStore, wg *sync.WaitGroup) {
	defer wg.Done() // İşçi tamamen kapandığında gruba haber ver

	for j := range jobs {
		store.SetStatus(j.ID, "Processing")
		fmt.Printf("👷 Worker %d processing -> Job: %d (%s)\n", id, j.ID, j.Payload)

		time.Sleep(5 * time.Second)

		store.SetStatus(j.ID, "Completed")
		fmt.Printf("✅ Worker %d finished   -> Job: %d\n", id, j.ID)
	}
}

func main() {
	const numWorkers = 3
	jobs := make(chan Job, 100)
	var wg sync.WaitGroup // İşçileri senkronize etmek için

	store := &JobStore{
		statuses: make(map[int]string),
		nextID:   1,
	}

	fmt.Println("🚀 Booting up Worker Pool...")
	for w := 1; w <= numWorkers; w++ {
		wg.Add(1)
		go worker(w, jobs, store, &wg)
	}

	// Route'ları yönetmek için Mux oluşturuyoruz
	mux := http.NewServeMux()

	mux.HandleFunc("/add", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
			return
		}

		var req struct {
			Payload string `json:"payload"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, "Invalid JSON", http.StatusBadRequest)
			return
		}

		jobID, status := store.AddJob(req.Payload)

		select {
		case jobs <- Job{ID: jobID, Payload: req.Payload}:
			w.Header().Set("Content-Type", "application/json")
			json.NewEncoder(w).Encode(map[string]interface{}{
				"id":     jobID,
				"status": status,
			})
		default:
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusServiceUnavailable)
			w.Write([]byte(`{"error": "Sistem şu an çok yoğun, lütfen daha sonra tekrar deneyin."}`))
		}
	})

	mux.HandleFunc("/status", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
			return
		}

		idStr := r.URL.Query().Get("id")
		jobID, err := strconv.Atoi(idStr)
		if err != nil {
			http.Error(w, "Invalid Job ID", http.StatusBadRequest)
			return
		}

		status, exists := store.GetStatus(jobID)
		if !exists {
			http.Error(w, "Job not found", http.StatusNotFound)
			return
		}

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]interface{}{
			"id":     jobID,
			"status": status,
		})
	})

	// HTTP sunucusunu bir objeye atıyoruz ki sonradan kapatabilelim
	srv := &http.Server{
		Addr:    ":8080",
		Handler: mux,
	}

	// Sunucuyu arka planda (goroutine) başlatıyoruz
	go func() {
		fmt.Println("🌐 Server is running on http://localhost:8080...")
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			fmt.Printf("HTTP server error: %v\n", err)
		}
	}()

	// GRACEFUL SHUTDOWN (GÜVENLİ KAPANIŞ) MİMARİSİ
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, os.Interrupt, syscall.SIGTERM) // Ctrl+C sinyalini dinle
	<-quit                                             // Sinyal gelene kadar ana thread burada bekler (bloklanır)

	fmt.Println("\n🛑 Shutdown signal received, initiating graceful shutdown...")

	// 1. Yeni HTTP isteklerini reddet (Mevcutların bitmesi için max 5 saniye ver)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	srv.Shutdown(ctx)

	// 2. Kanalları kapat ki işçiler yeni iş aramasın
	close(jobs)

	// 3. İçeride hala çalışmakta olan işçilerin bitirmesini bekle
	fmt.Println("⏳ Waiting for active workers to finish their tasks...")
	wg.Wait()

	fmt.Println("✨ System shutdown successfully without data loss.")
}
