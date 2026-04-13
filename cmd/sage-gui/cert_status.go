package main

import (
	"crypto/sha256"
	"fmt"
	"path/filepath"
	"time"

	"github.com/l33tdawg/sage/internal/tlsca"
)

// runCertStatus displays TLS certificate information for this node.
func runCertStatus() error {
	home := SageHome()
	certsDir := filepath.Join(home, "certs")

	if !tlsca.CertsExist(certsDir) {
		fmt.Println("No TLS certificates found.")
		fmt.Printf("  Expected location: %s\n", certsDir)
		fmt.Println()
		fmt.Println("Generate certificates by running:")
		fmt.Println("  sage-gui quorum-init --name <name> --address <host:port>")
		return nil
	}

	fmt.Println("SAGE TLS Certificate Status")
	fmt.Println("===========================")
	fmt.Println()

	// CA certificate
	caPath := filepath.Join(certsDir, tlsca.CACertFile)
	caCert, err := tlsca.ReadCert(caPath)
	if err != nil {
		fmt.Printf("  CA Certificate: ERROR — %v\n", err)
	} else {
		fingerprint := sha256.Sum256(caCert.Raw)
		fmt.Printf("  CA Certificate\n")
		fmt.Printf("    Subject:     %s\n", caCert.Subject.CommonName)
		fmt.Printf("    Not Before:  %s\n", caCert.NotBefore.Format(time.RFC3339))
		fmt.Printf("    Not After:   %s\n", caCert.NotAfter.Format(time.RFC3339))
		fmt.Printf("    Fingerprint: %X\n", fingerprint[:16])
		printExpiry(caCert.NotAfter)
	}

	fmt.Println()

	// Node certificate
	nodePath := filepath.Join(certsDir, tlsca.NodeCertFile)
	nodeCert, err := tlsca.ReadCert(nodePath)
	if err != nil {
		fmt.Printf("  Node Certificate: ERROR — %v\n", err)
	} else {
		fingerprint := sha256.Sum256(nodeCert.Raw)
		fmt.Printf("  Node Certificate\n")
		fmt.Printf("    Subject:     %s\n", nodeCert.Subject.CommonName)
		fmt.Printf("    Not Before:  %s\n", nodeCert.NotBefore.Format(time.RFC3339))
		fmt.Printf("    Not After:   %s\n", nodeCert.NotAfter.Format(time.RFC3339))
		fmt.Printf("    Fingerprint: %X\n", fingerprint[:16])
		if len(nodeCert.IPAddresses) > 0 {
			fmt.Printf("    IP SANs:     ")
			for i, ip := range nodeCert.IPAddresses {
				if i > 0 {
					fmt.Print(", ")
				}
				fmt.Print(ip)
			}
			fmt.Println()
		}
		if len(nodeCert.DNSNames) > 0 {
			fmt.Printf("    DNS SANs:    ")
			for i, dns := range nodeCert.DNSNames {
				if i > 0 {
					fmt.Print(", ")
				}
				fmt.Print(dns)
			}
			fmt.Println()
		}
		printExpiry(nodeCert.NotAfter)
	}

	fmt.Println()
	fmt.Printf("  Certs directory: %s\n", certsDir)

	return nil
}

func printExpiry(notAfter time.Time) {
	remaining := time.Until(notAfter)
	days := int(remaining.Hours() / 24)
	if days < 0 {
		fmt.Printf("    Status:      EXPIRED (%d days ago)\n", -days)
	} else if days < 30 {
		fmt.Printf("    Status:      WARNING — expires in %d days\n", days)
	} else {
		fmt.Printf("    Status:      Valid (%d days remaining)\n", days)
	}
}
