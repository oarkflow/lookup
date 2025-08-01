package main

import (
	"context"
	"log"
	"time"

	"github.com/oarkflow/lookup"
)

func main() {
	log.Println("🚀 Starting High-Performance Search Engine Manager Demo...")

	// Create high-performance manager with custom configuration
	config := &lookup.ManagerConfig{
		MaxWorkers:           8,
		RequestQueueSize:     10000,
		AutoOptimizeInterval: 30 * time.Minute,
		HealthCheckInterval:  5 * time.Minute,
		PersistenceEnabled:   true,
		PersistencePath:      "./data/indexes",
		CacheSize:            50000,
		CacheExpiry:          2 * time.Hour,
	}

	manager := lookup.NewHighPerformanceManager(config)
	defer manager.Close()

	// Create multiple indexes for different use cases
	indexes := []string{"documents", "products", "users", "logs"}

	for _, indexName := range indexes {
		if err := manager.CreateIndex(indexName); err != nil {
			log.Printf("Error creating index %s: %v", indexName, err)
			continue
		}
		log.Printf("✅ Created index: %s", indexName)
	}

	// Sample data for different indexes
	documentData := []lookup.GenericRecord{
		{
			"id":          1,
			"title":       "Advanced Machine Learning Techniques",
			"content":     "Comprehensive guide to deep learning, neural networks, and AI algorithms",
			"category":    "Technology",
			"author":      "Dr. Sarah Johnson",
			"publishDate": "2024-01-15",
			"rating":      4.8,
			"tags":        []string{"AI", "ML", "Deep Learning"},
		},
		{
			"id":          2,
			"title":       "High-Performance Computing with Go",
			"content":     "Building scalable applications using Go programming language and concurrent patterns",
			"category":    "Programming",
			"author":      "Mike Chen",
			"publishDate": "2024-02-20",
			"rating":      4.6,
			"tags":        []string{"Go", "Concurrency", "Performance"},
		},
		{
			"id":          3,
			"title":       "Database Design and Optimization",
			"content":     "Advanced techniques for designing efficient database schemas and query optimization",
			"category":    "Database",
			"author":      "Alex Rodriguez",
			"publishDate": "2024-03-10",
			"rating":      4.7,
			"tags":        []string{"Database", "SQL", "Optimization"},
		},
	}

	productData := []lookup.GenericRecord{
		{
			"id":          101,
			"name":        "Ultra-Fast SSD Drive",
			"description": "High-performance NVMe SSD with lightning-fast read/write speeds",
			"category":    "Storage",
			"price":       299.99,
			"brand":       "TechCorp",
			"inStock":     true,
			"specifications": map[string]interface{}{
				"capacity": "1TB",
				"speed":    "7000MB/s",
				"warranty": "5 years",
			},
		},
		{
			"id":          102,
			"name":        "AI-Powered Graphics Card",
			"description": "Next-generation GPU with AI acceleration and ray tracing capabilities",
			"category":    "Graphics",
			"price":       1299.99,
			"brand":       "GraphicsMax",
			"inStock":     true,
			"specifications": map[string]interface{}{
				"memory": "24GB GDDR6X",
				"cores":  10240,
				"power":  "350W",
			},
		},
	}

	userData := []lookup.GenericRecord{
		{
			"id":         1001,
			"username":   "john_doe",
			"email":      "john@example.com",
			"fullName":   "John Doe",
			"role":       "admin",
			"department": "Engineering",
			"joinDate":   "2023-06-15",
			"skills":     []string{"Go", "Python", "Kubernetes", "Docker"},
			"active":     true,
		},
		{
			"id":         1002,
			"username":   "jane_smith",
			"email":      "jane@example.com",
			"fullName":   "Jane Smith",
			"role":       "developer",
			"department": "Product",
			"joinDate":   "2023-08-20",
			"skills":     []string{"JavaScript", "React", "Node.js", "MongoDB"},
			"active":     true,
		},
	}

	// Index the data
	ctx := context.Background()

	log.Println("📚 Indexing sample data...")

	if err := manager.Build(ctx, "documents", documentData); err != nil {
		log.Printf("Error indexing documents: %v", err)
	} else {
		log.Println("✅ Documents indexed successfully")
	}

	if err := manager.Build(ctx, "products", productData); err != nil {
		log.Printf("Error indexing products: %v", err)
	} else {
		log.Println("✅ Products indexed successfully")
	}

	if err := manager.Build(ctx, "users", userData); err != nil {
		log.Printf("Error indexing users: %v", err)
	} else {
		log.Println("✅ Users indexed successfully")
	}

	// Wait a moment for indexing to complete
	time.Sleep(500 * time.Millisecond)

	// Demonstrate search capabilities
	log.Println("\n🔍 Demonstrating search capabilities...")

	// Search documents
	documentQuery := lookup.Request{
		Query: "machine learning AI",
		Size:  10,
	}

	if result, err := manager.Search(ctx, "documents", documentQuery); err != nil {
		log.Printf("Document search error: %v", err)
	} else {
		log.Printf("📄 Document search for 'machine learning AI' found %d results", result.Total)
		for i, item := range result.Items {
			if i >= 2 { // Show only first 2 results
				break
			}
			log.Printf("  - %s (Score: %.3f)", item.Record["title"], item.Score)
		}
	}

	// Search products
	productQuery := lookup.Request{
		Query: "performance SSD graphics",
		Size:  5,
	}

	if result, err := manager.Search(ctx, "products", productQuery); err != nil {
		log.Printf("Product search error: %v", err)
	} else {
		log.Printf("🛍️  Product search for 'performance SSD graphics' found %d results", result.Total)
		for i, item := range result.Items {
			log.Printf("  - %s ($%.2f) - Score: %.3f",
				item.Record["name"], item.Record["price"], item.Score)
		}
	}

	// Search users
	userQuery := lookup.Request{
		Query: "developer engineering Go",
		Size:  5,
	}

	if result, err := manager.Search(ctx, "users", userQuery); err != nil {
		log.Printf("User search error: %v", err)
	} else {
		log.Printf("👥 User search for 'developer engineering Go' found %d results", result.Total)
		for _, item := range result.Items {
			log.Printf("  - %s (%s) - %s - Score: %.3f",
				item.Record["fullName"], item.Record["username"],
				item.Record["department"], item.Score)
		}
	}

	// Display system metrics
	log.Println("\n📊 System Metrics:")
	metrics := manager.GetSystemMetrics()
	log.Printf("  Total Indexes: %v", metrics["total_indexes"])
	log.Printf("  Total Documents: %v", metrics["total_documents"])
	log.Printf("  Total Terms: %v", metrics["total_terms"])
	log.Printf("  Total Queries: %v", metrics["total_queries"])
	log.Printf("  Worker Count: %v", metrics["worker_count"])
	log.Printf("  Uptime: %v", metrics["uptime"])

	// Show detailed index statistics
	log.Println("\n📈 Index Statistics:")
	indexStats := manager.ListIndexes()
	for name, stats := range indexStats {
		log.Printf("  %s:", name)
		log.Printf("    Documents: %d", stats.DocumentCount)
		log.Printf("    Terms: %d", stats.TermCount)
		log.Printf("    Queries: %d", stats.TotalQueries)
		log.Printf("    Avg Latency: %v", stats.AverageLatency)
		log.Printf("    Last Accessed: %v", stats.LastAccessed.Format("2006-01-02 15:04:05"))
	}

	// Start the advanced HTTP server
	log.Println("\n🌐 Starting Advanced HTTP Server...")
	log.Println("Available endpoints:")
	log.Println("  - GET  / - Web UI")
	log.Println("  - GET  /api/indexes - List all indexes")
	log.Println("  - POST /api/index/create?name=<name> - Create index")
	log.Println("  - DELETE /api/index/<name> - Delete index")
	log.Println("  - GET  /api/search/<index>?q=<query> - Search index")
	log.Println("  - GET  /api/metrics - System metrics")
	log.Println("\n🎯 Open http://localhost:8080/search-ui.html for the web interface")

	// This will block and serve HTTP requests
	manager.StartAdvancedHTTPServer(":8080")
}
