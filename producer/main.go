package main

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"

	"github.com/mehedimayall/kafka-go/kafkaClient"
)

type Order struct {
	CustomerName string `json:"customer_name"`
	CoffeeType   string `json:"coffee_type"`
}

func print[T any](values T) {
	fmt.Println(values)
}

func main() {
	http.HandleFunc("/order", placeOrder)
	print("Server is running at the port 3300")

	err := http.ListenAndServe(":3300", nil)
	if err != nil {
		log.Fatalln(err)
	}
}

func placeOrder(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Invalid request method", http.StatusMethodNotAllowed)
		return
	}

	// 1. Parse request body into order
	order := new(Order)

	err := json.NewDecoder(r.Body).Decode(order)
	if err != nil {
		log.Println(err)
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	// 2. Convert body into bytes
	orderInBytes, err := json.Marshal(order)
	if err != nil {
		log.Println(err)
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	// 3. Send the bytes to kafka
	err = PushOrderIntoTheQueue("coffee_orders", orderInBytes)
	if err != nil {
		log.Println(err)
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	// 4. Respond back to the user
	response := map[string]interface{}{
		"success": true,
		"msg":     "Order for " + order.CustomerName + " placed successfully",
	}

	w.Header().Set("Content-Type", "application/json")

	if err := json.NewEncoder(w).Encode(response); err != nil {
		log.Println(err)
		http.Error(w, "Error placing order", http.StatusInternalServerError)
		return
	}

}

func PushOrderIntoTheQueue(topic string, message []byte) error {
	return kafkaClient.PushIntoTheQueue(topic, message)
}
