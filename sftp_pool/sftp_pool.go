package sftp_pool

import (
	"cliback/config"
	"errors"
	"github.com/pkg/sftp"
	"golang.org/x/crypto/ssh"
	"strconv"
	"sync"
)

var (
	once sync.Once
	sftp_pool_instance *SftpPool
)

type SftpConn struct {
	ssh_cli *ssh.Client
	sftp_cli *sftp.Client
	in_use *bool
}

type SftpPool struct {
	pool []SftpConn
	max_conn int
	conn_in_use int
	conn_opened int
	ssh_config map[string]string
	mux sync.Mutex
}

func New() *SftpPool {
	once.Do(func() {
		sftp_pool_instance = new(SftpPool)
		sftp_pool_instance.max_conn=10
		c:=config.New()
		sftp_pool_instance.ssh_config=make(map[string]string)
		sftp_pool_instance.ssh_config["remote"]=c.BackupStorage.BackupConn.HostName
		sftp_pool_instance.ssh_config["port"]=strconv.FormatUint(uint64(c.BackupStorage.BackupConn.Port),10)
		sftp_pool_instance.ssh_config["user"]=c.BackupStorage.BackupConn.UserName
		sftp_pool_instance.ssh_config["pass"]=c.BackupStorage.BackupConn.Password
		sftp_pool_instance.ssh_config["public_key"]=c.BackupStorage.BackupConn.KeyFilename
	})
	return sftp_pool_instance
}

func (sp *SftpPool) SetMaxConn(max_conn int){
	sp.max_conn=max_conn
}

func (sp *SftpPool) SetSSHConfig(Ssh_Config map[string]string){
	sp.ssh_config=Ssh_Config
}

func (sp *SftpPool) GetClient() (*sftp.Client,error){
	sp.mux.Lock()
	defer sp.mux.Unlock()
	if(sp.max_conn <= sp.conn_in_use){
		return nil,errors.New("No free connection, retry later")
	}
	if(sp.conn_opened>sp.conn_in_use){
		// Search unused connections
		for _,p := range sp.pool{
			if *p.in_use { continue }
			// Add here check connection
			*p.in_use = true
			sp.conn_in_use++
			return p.sftp_cli,nil
		}
	}
	ssh_cli,sftp_cli,err:=MakeConnection(sp.ssh_config)
	if err!=nil{
		return nil, err
	}
	bt:= new(bool)
	*bt=true
	sp.pool=append(sp.pool, SftpConn{
		ssh_cli:  ssh_cli,
		sftp_cli: sftp_cli,
		in_use:   bt,
	})
	sp.conn_in_use++
	sp.conn_opened++
	return sftp_cli,nil
}
func (sp *SftpPool) ReleaseClient(sftp_client *sftp.Client) error{
	sp.mux.Lock()
	defer sp.mux.Unlock()
	for _,p := range sp.pool{
		if p.sftp_cli == sftp_client{
			*p.in_use=false
			sp.conn_in_use--
			// Check connection
			return nil
		}
	}
	return nil
}

type sftp_release_closer struct {
	sftp_pool *SftpPool
	sftp_cli *sftp.Client
}

func (src *sftp_release_closer) Close() error{
	err := src.sftp_pool.ReleaseClient(src.sftp_cli)
	return err
}

func (sp *SftpPool) MakeReleaseCloser(sftp_cli *sftp.Client) (*sftp_release_closer){
	return &sftp_release_closer{
		sftp_pool: sp,
		sftp_cli:  sftp_cli,
	}
}