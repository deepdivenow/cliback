package sftp_pool

import (
	"bufio"
	"errors"
	"github.com/pkg/sftp"
	"golang.org/x/crypto/ssh"
	"io/ioutil"
	"log"
	"os"
	"path/filepath"
	"strings"
)

func MakeSshClientConfig(c map[string]string) ssh.ClientConfig {
	var ssh_auth []ssh.AuthMethod
	if pass, ok := c["pass"]; ok {
		ssh_auth = append(ssh_auth, ssh.Password(pass))
	}
	var pkey_paths []string
	if pkey_path, ok := c["public_key"]; ok {
		pkey_paths = append(pkey_paths, pkey_path)
	}
	pkey_paths = append(pkey_paths, "~/.ssh/id_rsa")
	pkey_paths = append(pkey_paths, "~/.ssh/id_sda")
	for _, pkey_path := range pkey_paths {
		if pkey, err := publicKey(pkey_path); err == nil {
			ssh_auth = append(ssh_auth, pkey)
		}
	}

	clientConfig := ssh.ClientConfig{
		User:            c["user"],
		Auth:            ssh_auth,
		HostKeyCallback: ssh.InsecureIgnoreHostKey(),
		//HostKeyCallback: ssh.FixedHostKey(hostKey),
	}
	clientConfig.SetDefaults()
	clientConfig.Ciphers = append(clientConfig.Ciphers, "diffie-hellman-group-exchange-sha256")
	return clientConfig
}

func publicKey(path string) (ssh.AuthMethod, error) {
	key, err := ioutil.ReadFile(path)
	if err != nil {
		return nil, err
	}
	signer, err := ssh.ParsePrivateKey(key)
	if err != nil {
		return nil, err
	}
	return ssh.PublicKeys(signer), nil
}

func MakeConnection(cmap map[string]string) (*ssh.Client, *sftp.Client, error) {

	for _, key := range []string{"user", "remote", "port"} {
		if _, ok := cmap[key]; !ok {
			return nil, nil, errors.New("Set all keys for ssh connection")
		}
	}
	// get host public key
	//hostKey := getHostKey(remote)
	config := MakeSshClientConfig(cmap)

	// connect
	ssh_client, err := ssh.Dial("tcp", cmap["remote"]+":"+cmap["port"], &config)
	if err != nil {
		return nil, nil, err
	}
	// create new SFTP client
	sftp_client, err := sftp.NewClient(ssh_client)
	if err != nil {
		ssh_client.Close()
		return nil, nil, err
	}
	return ssh_client, sftp_client, nil
	//defer client.Close()
	//
	//// create destination file
	//dstFile, err := client.Create("/tmp/algo_2020_1.zip")
	//if err != nil {
	//	log.Fatal(err)
	//}
	//defer dstFile.Close()
	//
	//// create source file
	//srcFile, err := os.Open("/home/dro/DOCK_VIDEO/OTUS/algo_2020_1.zip")
	//if err != nil {
	//	log.Fatal(err)
	//}
	//
	//// copy source file to destination file
	//bytes, err := io.Copy(dstFile, srcFile)
	//if err != nil {
	//	log.Fatal(err)
	//}
	//fmt.Printf("%d bytes copied\n", bytes)
}

func getHostKey(host string) ssh.PublicKey {
	// parse OpenSSH known_hosts file
	// ssh or use ssh-keyscan to get initial key
	file, err := os.Open(filepath.Join(os.Getenv("HOME"), ".ssh", "known_hosts"))
	if err != nil {
		log.Fatal(err)
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	var hostKey ssh.PublicKey
	for scanner.Scan() {
		fields := strings.Split(scanner.Text(), " ")
		if len(fields) != 3 {
			continue
		}
		if strings.Contains(fields[0], host) {
			var err error
			hostKey, _, _, _, err = ssh.ParseAuthorizedKey(scanner.Bytes())
			if err != nil {
				log.Fatalf("error parsing %q: %v", fields[2], err)
			}
			break
		}
	}

	if hostKey == nil {
		log.Fatalf("no hostkey found for %s", host)
	}

	return hostKey
}
