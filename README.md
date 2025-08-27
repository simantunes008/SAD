# SAD

## Executar o NameNode
```bash
./metaserver <IPs dos DataNodes>
```
Ex:
```bash
./metaserver 127.0.0.1:5001 127.0.0.1:5002
```

## Executar um DataNode
```bash
./dataserver <IP do NameNode>
```
Caso seja em localhost é necessário mudar os ports e a pasta onde vão ser guardados os ficheiros
Ex:
```bash
./dataserver 127.0.0.1:5000
```

## Iniciar o Fuse
```bash
sudo ./passthrough /mnt/fs -omodules="subdir,subdir=/backend" -oallow_other -f
```
Usar --server para especificar o IP do NameNode (por defeito server=127.0.0.1:5000)
Ex:
```bash
sudo ./passthrough /mnt/fs -omodules="subdir,subdir=/backend" -oallow_other -f --server=127.0.0.1:5000
```