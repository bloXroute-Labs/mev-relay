package common

import (
	"crypto/tls"
	"fmt"
)

func CreateKeyPair(certificatesPath string) (tls.Certificate, error) {
	keyPair, err := tls.LoadX509KeyPair(certificatesPath+"/api_cert.pem", certificatesPath+"/api_key.pem")
	if err != nil {
		return tls.Certificate{}, fmt.Errorf("there was an error creating an X509KeyPair from the certificate and the key: %v", err)
	}

	return keyPair, nil
}
