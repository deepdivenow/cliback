package sftp_pool

import (
	"bufio"
	"errors"
	"io/ioutil"
	"log"
	"os"
	"path"
	"path/filepath"
	"strings"

	"github.com/pkg/sftp"
	"golang.org/x/crypto/ssh"
)

func MakeSshClientConfig(c map[string]string) ssh.ClientConfig {
	var sshAuth []ssh.AuthMethod
	if pass, ok := c["pass"]; ok {
		sshAuth = append(sshAuth, ssh.Password(pass))
	}
	var pkeyPaths []string
	if pkeyPath, ok := c["public_key"]; ok {
		if len(pkeyPath) > 0 {
			pkeyPaths = append(pkeyPaths, pkeyPath)
		}
	}
	homePath, exists := os.LookupEnv("HOME")
	if exists {
		for _, p := range []string{path.Join(homePath, ".ssh/id_rsa"), path.Join(homePath, ".ssh/id_dsa")} {
			fi, err := os.Stat(p)
			if err == nil && !fi.IsDir() {
				pkeyPaths = append(pkeyPaths, p)
			}
		}
	}
	for _, pkeyPath := range pkeyPaths {
		if pkey, err := publicKey(pkeyPath); err == nil {
			sshAuth = append(sshAuth, pkey)
		}
	}

	clientConfig := ssh.ClientConfig{
		User:            c["user"],
		Auth:            sshAuth,
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
	sshClient, err := ssh.Dial("tcp", cmap["remote"]+":"+cmap["port"], &config)
	if err != nil {
		return nil, nil, err
	}
	// create new SFTP client
	sftpClient, err := sftp.NewClient(sshClient)
	if err != nil {
		sshClient.Close()
		return nil, nil, err
	}
	return sshClient, sftpClient, nil
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

func RemoveDirectoryRecursive(sftpClient *sftp.Client, remotePath string) error {
	remoteFiles, err := sftpClient.ReadDir(remotePath)
	if err != nil {
		return err
	}
	defer sftpClient.RemoveDirectory(remotePath)
	for _, backupDir := range remoteFiles {
		remoteFilePath := path.Join(remotePath, backupDir.Name())
		if backupDir.IsDir() {
			err = RemoveDirectoryRecursive(sftpClient, remoteFilePath)
			if err != nil {
				return err
			}
		} else {
			err = sftpClient.Remove(path.Join(remoteFilePath))
			if err != nil {
				return err
			}
		}
	}
	return nil
}
