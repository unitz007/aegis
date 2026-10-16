// Command sealkey makes the key pair aegis uses to receive broker logins.
//
//	go run ./cmd/sealkey
//
// Put the private line in aegis's secrets (fly secrets set ACCOUNT_SEAL_KEY=...).
// The public key is published by aegis at GET /account/key; senders seal to it.
package main

import (
	"encoding/base64"
	"fmt"
	"log"

	"github.com/unitz007/aegis/sealed"
)

func main() {
	private, public, err := sealed.GenerateKey()
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println("# Keep this secret. Set it on aegis only:")
	fmt.Printf("ACCOUNT_SEAL_KEY=%s\n", base64.StdEncoding.EncodeToString(private))
	fmt.Println("# Public. Safe to share; aegis also serves it at /account/key:")
	fmt.Printf("public_key=%s\n", base64.StdEncoding.EncodeToString(public))
}
