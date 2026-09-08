package main

import "fmt"

// insertionSort sorts the slice in place.
func insertionSort(numbers []int) {
	for i := 1; i < len(numbers); i++ {
		key := numbers[i]
		j := i - 1

		// Shift larger elements one position to the right.
		for j >= 0 && numbers[j] > key {
			numbers[j+1] = numbers[j]
			j--
		}

		// Insert key into its correct position.
		numbers[j+1] = key
	}
}

func main() {
	numbers := []int{12, 11, 13, 5, 6}

	insertionSort(numbers)

	fmt.Println(numbers)
}
