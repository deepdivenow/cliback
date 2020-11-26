package sftp_pool

import (
	"cliback/config"
	"container/list"
	"errors"
	"log"
	"strconv"
	"sync"
	"time"

	"github.com/pkg/sftp"
	"golang.org/x/crypto/ssh"
)

var (
	once             sync.Once
	sftpPoolInstance *SftpPool
)

const (
	maxConnDuration = 300 * time.Second
)

type SftpConn struct {
	sshCli    *ssh.Client
	sftpCli   *sftp.Client
	inUse     *bool
	startTime time.Time
}

type SftpPool struct {
	pool       *list.List
	maxConn    int
	connInUse  int
	connOpened int
	sshConfig  map[string]string
	mux        sync.Mutex
}

func New() *SftpPool {
	once.Do(func() {
		c := config.New()
		sftpPoolInstance = new(SftpPool)
		sftpPoolInstance.maxConn = c.WorkerPool.NumWorkers + 2
		sftpPoolInstance.sshConfig = make(map[string]string)
		sftpPoolInstance.sshConfig["remote"] = c.BackupStorage.BackupConn.HostName
		sftpPoolInstance.sshConfig["port"] = strconv.FormatUint(uint64(c.BackupStorage.BackupConn.Port), 10)
		sftpPoolInstance.sshConfig["user"] = c.BackupStorage.BackupConn.UserName
		sftpPoolInstance.sshConfig["pass"] = c.BackupStorage.BackupConn.Password
		sftpPoolInstance.sshConfig["public_key"] = c.BackupStorage.BackupConn.KeyFilename
		sftpPoolInstance.pool = new(list.List)
	})
	return sftpPoolInstance
}

func (sp *SftpPool) SetMaxConn(maxConn int) {
	sp.maxConn = maxConn
}

func (sp *SftpPool) SetSSHConfig(SshConfig map[string]string) {
	sp.sshConfig = SshConfig
}

func (sp *SftpPool) GetClient() (*sftp.Client, error) {
	sp.mux.Lock()
	defer sp.mux.Unlock()
	if sp.maxConn <= sp.connInUse {
		return nil, errors.New("No free connection, retry later")
	}
	if sp.connOpened > sp.connInUse {
		// Search unused connections
		for e := sp.pool.Front(); e != nil; e = e.Next() {
			p := e.Value.(SftpConn)
			if *p.inUse {
				continue
			}
			// Add here check connection
			*p.inUse = true
			sp.connInUse++
			return p.sftpCli, nil
		}
	}
	sshCli, sftpCli, err := MakeConnection(sp.sshConfig)
	if err != nil {
		return nil, err
	}
	bt := new(bool)
	*bt = true
	sp.pool.PushBack(SftpConn{
		sshCli:    sshCli,
		sftpCli:   sftpCli,
		inUse:     bt,
		startTime: time.Now(),
	})
	sp.connInUse++
	sp.connOpened++
	return sftpCli, nil
}

func (sp *SftpPool) GetClientLoop() (*sftp.Client, error) {
	for {
		sftpClient, err := sp.GetClient()
		if err != nil {
			log.Printf("Error Get SFTP Client: %s", err)
			time.Sleep(time.Second * 5)
			continue
		}
		return sftpClient, err
	}
}

func (sp *SftpPool) CheckConnection(sftpClient *sftp.Client) error {
	c := config.New()
	_, err := sftpClient.Stat(c.BackupStorage.BackupDir)
	return err
}

func (sp *SftpPool) RemoveConnection(e *list.Element) error {
	p := e.Value.(SftpConn)
	sp.connOpened--
	sp.connInUse--
	p.sftpCli.Close()
	p.sshCli.Close()
	sp.pool.Remove(e)
	return nil
}

func (sp *SftpPool) ReleaseClient(sftpClient *sftp.Client) error {
	for e := sp.pool.Front(); e != nil; e = e.Next() {
		p := e.Value.(SftpConn)
		if p.sftpCli == sftpClient {
			connStatus := sp.CheckConnection(sftpClient)
			//var connStatus error = nil
			if connStatus != nil ||
				time.Now().Sub(p.startTime.Add(maxConnDuration)) > 0 {
				sp.mux.Lock()
				defer sp.mux.Unlock()
				sp.RemoveConnection(e)
				return nil
			}
			sp.mux.Lock()
			defer sp.mux.Unlock()
			*p.inUse = false
			sp.connInUse--
			return nil
		}
	}
	return nil
}

type sftpReleaseCloser struct {
	sftpPool *SftpPool
	sftpCli  *sftp.Client
}

func (src *sftpReleaseCloser) Close() error {
	err := src.sftpPool.ReleaseClient(src.sftpCli)
	return err
}

func (sp *SftpPool) MakeReleaseCloser(sftpCli *sftp.Client) *sftpReleaseCloser {
	return &sftpReleaseCloser{
		sftpPool: sp,
		sftpCli:  sftpCli,
	}
}
