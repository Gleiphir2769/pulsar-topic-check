package main

import (
	"fmt"
	"testing"
)

func TestGetTopicFromFullName(t *testing.T) {
	fmt.Println(GetTopicFromFullName("persistent"))
}
