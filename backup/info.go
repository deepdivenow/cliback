package backup

import "cliback/transport"

func info() error{
	var bi_version int = 1;
	metas,err := transport.SearchMeta()
	if err != nil{
		return err
	}
	for _,back_name := range(metas){
		switch bi_version{
		case 1:
			bi1,err := Backupv1Read(back_name)
		case 2:
			bi2,err := Backupv2Read(back_name)
		default:
			return errors.New("Error read backup info version")
		}

	}
	return nil
}