package main

import (
	"database/sql"
	"fmt"
	"log"
	"path/filepath"
	"strings"

	_ "github.com/mattn/go-sqlite3"
)

/**
 * SQLite Demo Program - Go
 * Demonstrates major SQLite operations: CREATE, INSERT, SELECT, UPDATE, DELETE
 */

type User struct {
	ID        int
	Name      string
	Email     string
	Age       sql.NullInt64
	CreatedAt string
}

type Product struct {
	ID       int
	Name     string
	Price    float64
	Category sql.NullString
	Stock    sql.NullInt64
}

type SqliteDemo struct {
	db *sql.DB
}

func main() {
	demo := &SqliteDemo{}
	if err := demo.Connect(); err != nil {
		log.Fatal("Failed to connect to database:", err)
	}
	defer demo.Disconnect()

	if err := demo.RunDemo(); err != nil {
		log.Fatal("Demo failed:", err)
	}
}

func (demo *SqliteDemo) Connect() error {
	dbPath := filepath.Join("..", "sqlite1.db")
	absPath, err := filepath.Abs(dbPath)
	if err != nil {
		return err
	}

	db, err := sql.Open("sqlite3", absPath)
	if err != nil {
		return err
	}

	demo.db = db
	fmt.Println("=== SQLite Go Demo ===")
	fmt.Printf("Connected to: %s\n\n", absPath)
	return nil
}

func (demo *SqliteDemo) Disconnect() {
	if demo.db != nil {
		demo.db.Close()
		fmt.Println("\nDatabase connection closed.")
	}
}

func (demo *SqliteDemo) RunDemo() error {

	// 1. Create tables if they don't exist
	if err := demo.createTables(); err != nil {
		return err
	}

	// 2. INSERT operations
	if err := demo.insertOperations(); err != nil {
		return err
	}

	// 1. SELECT operations
	if err := demo.selectOperations(); err != nil {
		return err
	}

	// 3. UPDATE operations
	if err := demo.updateOperations(); err != nil {
		return err
	}

	// 4. DELETE operations
	if err := demo.deleteOperations(); err != nil {
		return err
	}

	// 5. Aggregate functions
	if err := demo.aggregateFunctions(); err != nil {
		return err
	}

	// 6. Transaction example
	if err := demo.transactionExample(); err != nil {
		return err
	}

	// 7. Final state
	if err := demo.finalState(); err != nil {
		return err
	}

	return nil
}

func (demo *SqliteDemo) createTables() error {
	fmt.Println("Creating tables if they don't exist...")
	fmt.Println(strings.Repeat("-", 30))

	// Create users table
	createUsersTable := `
	CREATE TABLE IF NOT EXISTS users (
		id INTEGER PRIMARY KEY AUTOINCREMENT,
		name TEXT NOT NULL,
		email TEXT UNIQUE NOT NULL,
		age INTEGER,
		created_at TEXT DEFAULT CURRENT_TIMESTAMP
	);`
	
	_, err := demo.db.Exec(createUsersTable)
	if err != nil {
		return fmt.Errorf("failed to create users table: %v", err)
	}

	// Create products table
	createProductsTable := `
	CREATE TABLE IF NOT EXISTS products (
		id INTEGER PRIMARY KEY AUTOINCREMENT,
		name TEXT NOT NULL,
		price REAL NOT NULL,
		category TEXT,
		stock INTEGER
	);`
	
	_, err = demo.db.Exec(createProductsTable)
	if err != nil {
		return fmt.Errorf("failed to create products table: %v", err)
	}

	// Insert initial users if table is empty
	var userCount int
	err = demo.db.QueryRow("SELECT COUNT(*) FROM users").Scan(&userCount)
	if err != nil {
		return fmt.Errorf("failed to check user count: %v", err)
	}

	if userCount == 0 {
		// Insert some initial users
		initialUsers := []struct {
			name  string
			email string
			age   int
		}{
			{"Alice Johnson", "alice@example.com", 28},
			{"Bob Smith", "bob@example.com", 32},
			{"Carol Davis", "carol@example.com", 25},
		}

		for _, user := range initialUsers {
			_, err := demo.db.Exec("INSERT INTO users (name, email, age) VALUES (?, ?, ?)",
				user.name, user.email, user.age)
			if err != nil {
				return fmt.Errorf("failed to insert initial user %s: %v", user.name, err)
			}
		}
		fmt.Println("Inserted initial users")
	}

	// Insert initial products if table is empty
	var productCount int
	err = demo.db.QueryRow("SELECT COUNT(*) FROM products").Scan(&productCount)
	if err != nil {
		return fmt.Errorf("failed to check product count: %v", err)
	}

	if productCount == 0 {
		// Insert some initial products
		initialProducts := []struct {
			name     string
			price    float64
			category string
			stock    int
		}{
			{"Coffee Mug", 12.99, "Kitchen", 50},
			{"Book", 24.99, "Education", 100},
			{"Laptop Stand", 45.00, "Office", 25},
		}

		for _, product := range initialProducts {
			_, err := demo.db.Exec("INSERT INTO products (name, price, category, stock) VALUES (?, ?, ?, ?)",
				product.name, product.price, product.category, product.stock)
			if err != nil {
				return fmt.Errorf("failed to insert initial product %s: %v", product.name, err)
			}
		}
		fmt.Println("Inserted initial products")
	}

	fmt.Println("Tables ready!")
	fmt.Println()
	return nil
}

func (demo *SqliteDemo) selectOperations() error {
	fmt.Println("1. SELECT Operations:")
	fmt.Println(strings.Repeat("-", 30))

	// Get all users
	rows, err := demo.db.Query("SELECT * FROM users")
	if err != nil {
		return err
	}
	defer rows.Close()

	fmt.Println("All Users:")
	for rows.Next() {
		var user User
		if err := rows.Scan(&user.ID, &user.Name, &user.Email, &user.Age, &user.CreatedAt); err != nil {
			return err
		}
		age := "NULL"
		if user.Age.Valid {
			age = fmt.Sprintf("%d", user.Age.Int64)
		}
		fmt.Printf("  ID: %d, Name: %s, Email: %s, Age: %s\n", 
			user.ID, user.Name, user.Email, age)
	}
	fmt.Println()

	// Get users with condition
	rows, err = demo.db.Query("SELECT * FROM users WHERE age > 25")
	if err != nil {
		return err
	}
	defer rows.Close()

	fmt.Println("Users older than 25:")
	for rows.Next() {
		var user User
		if err := rows.Scan(&user.ID, &user.Name, &user.Email, &user.Age, &user.CreatedAt); err != nil {
			return err
		}
		if user.Age.Valid && user.Age.Int64 > 25 {
			fmt.Printf("  %s (Age: %d)\n", user.Name, user.Age.Int64)
		}
	}
	fmt.Println()

	// Get products with price > 20
	rows, err = demo.db.Query("SELECT * FROM products WHERE price > 20")
	if err != nil {
		return err
	}
	defer rows.Close()

	fmt.Println("Products priced above $20:")
	for rows.Next() {
		var product Product
		if err := rows.Scan(&product.ID, &product.Name, &product.Price, &product.Category, &product.Stock); err != nil {
			return err
		}
		stock := "NULL"
		if product.Stock.Valid {
			stock = fmt.Sprintf("%d", product.Stock.Int64)
		}
		fmt.Printf("  %s - $%.2f (Stock: %s)\n", product.Name, product.Price, stock)
	}
	fmt.Println()

	return nil
}

func (demo *SqliteDemo) insertOperations() error {
	fmt.Println("2. INSERT Operations:")
	fmt.Println(strings.Repeat("-", 30))

	// Insert a new user
	result, err := demo.db.Exec("INSERT INTO users (name, email, age) VALUES (?, ?, ?)",
		"David Wilson", "david@example.com", 31)
	if err != nil {
		return err
	}
	userID, _ := result.LastInsertId()
	fmt.Printf("Inserted new user: David Wilson (ID: %d)\n", userID)

	// Insert a new product
	result, err = demo.db.Exec("INSERT INTO products (name, price, category, stock) VALUES (?, ?, ?, ?)",
		"Smartphone", 699.99, "Electronics", 25)
	if err != nil {
		return err
	}
	productID, _ := result.LastInsertId()
	fmt.Printf("Inserted new product: Smartphone (ID: %d)\n", productID)
	fmt.Println()

	return nil
}

func (demo *SqliteDemo) updateOperations() error {
	fmt.Println("3. UPDATE Operations:")
	fmt.Println(strings.Repeat("-", 30))

	// Update user age
	result, err := demo.db.Exec("UPDATE users SET age = 29 WHERE name = 'Alice Johnson'")
	if err != nil {
		return err
	}
	rows, _ := result.RowsAffected()
	fmt.Printf("Updated Alice Johnson's age to 29 (Rows: %d)\n", rows)

	// Update product stock
	result, err = demo.db.Exec("UPDATE products SET stock = stock - 5 WHERE name = 'Coffee Mug'")
	if err != nil {
		return err
	}
	rows, _ = result.RowsAffected()
	fmt.Printf("Decreased Coffee Mug stock by 5 (Rows: %d)\n", rows)
	fmt.Println()

	return nil
}

func (demo *SqliteDemo) deleteOperations() error {
	fmt.Println("4. DELETE Operations:")
	fmt.Println(strings.Repeat("-", 30))

	// Delete a user (be careful with deletes!)
	result, err := demo.db.Exec("DELETE FROM users WHERE name = 'David Wilson'")
	if err != nil {
		return err
	}
	rows, _ := result.RowsAffected()
	fmt.Printf("Deleted user: David Wilson (Rows: %d)\n", rows)
	fmt.Println()

	return nil
}

func (demo *SqliteDemo) aggregateFunctions() error {
	fmt.Println("5. Aggregate Functions:")
	fmt.Println(strings.Repeat("-", 30))

	// Count users
	var userCount int
	err := demo.db.QueryRow("SELECT COUNT(*) FROM users").Scan(&userCount)
	if err != nil {
		return err
	}
	fmt.Printf("Total users: %d\n", userCount)

	// Average age
	var avgAge float64
	err = demo.db.QueryRow("SELECT AVG(age) FROM users").Scan(&avgAge)
	if err != nil {
		return err
	}
	fmt.Printf("Average user age: %.1f\n", avgAge)

	// Total stock
	var totalStock int
	err = demo.db.QueryRow("SELECT SUM(stock) FROM products").Scan(&totalStock)
	if err != nil {
		return err
	}
	fmt.Printf("Total product stock: %d\n", totalStock)

	// Max price
	var maxPrice float64
	err = demo.db.QueryRow("SELECT MAX(price) FROM products").Scan(&maxPrice)
	if err != nil {
		return err
	}
	fmt.Printf("Most expensive product: $%.2f\n", maxPrice)
	fmt.Println()

	return nil
}

func (demo *SqliteDemo) transactionExample() error {
	fmt.Println("6. Transaction Example:")
	fmt.Println(strings.Repeat("-", 30))

	tx, err := demo.db.Begin()
	if err != nil {
		return err
	}

	// Simulate a purchase
	_, err = tx.Exec("UPDATE products SET stock = stock - 1 WHERE name = 'Book'")
	if err != nil {
		tx.Rollback()
		return err
	}

	_, err = tx.Exec("INSERT INTO users (name, email, age) VALUES (?, ?, ?)",
		"Transaction Test", "test@example.com", 25)
	if err != nil {
		tx.Rollback()
		return err
	}

	// Commit transaction
	if err := tx.Commit(); err != nil {
		return err
	}
	fmt.Println("Transaction completed successfully!")
	fmt.Println()

	return nil
}

func (demo *SqliteDemo) finalState() error {
	fmt.Println("7. Final Database State:")
	fmt.Println(strings.Repeat("-", 30))

	rows, err := demo.db.Query("SELECT * FROM users ORDER BY id")
	if err != nil {
		return err
	}
	defer rows.Close()

	fmt.Println("Final Users:")
	for rows.Next() {
		var user User
		if err := rows.Scan(&user.ID, &user.Name, &user.Email, &user.Age, &user.CreatedAt); err != nil {
			return err
		}
		fmt.Printf("  %s (%s)\n", user.Name, user.Email)
	}

	rows, err = demo.db.Query("SELECT * FROM products ORDER BY id")
	if err != nil {
		return err
	}
	defer rows.Close()

	fmt.Println("\nFinal Products:")
	for rows.Next() {
		var product Product
		if err := rows.Scan(&product.ID, &product.Name, &product.Price, &product.Category, &product.Stock); err != nil {
			return err
		}
		stock := "NULL"
		if product.Stock.Valid {
			stock = fmt.Sprintf("%d", product.Stock.Int64)
		}
		fmt.Printf("  %s - $%.2f (Stock: %s)\n", product.Name, product.Price, stock)
	}

	return nil
}