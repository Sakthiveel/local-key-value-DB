package main

// type Animals struct {
// 	Name    string `json:"name"`
// 	Country string `json:"country"`
// 	Age     int    `json:"age"`
// }

// func NewAnimal(name string, country string, age int) Animals {
// 	return Animals{
// 		Name:    name,
// 		Country: country,
// 		Age:     age,
// 	}
// }

// func AnimalEntry(name string, country string, age int, ttlSeconds string) DbData[Animals] {
// 	return NewDbData[Animals](NewAnimal(name, country, age), ttlSeconds)
// }

func main() {
	// dir, _ := os.UserHomeDir()
	// dbsIns, err := NewDB[Animals]("animals", dir)

	// if err != nil {
	// 	fmt.Print(err)
	// }

	// dbsIns.create("1", AnimalEntry("Tiger", "Syberia", 4, ""))

}
