## 1.1.0 (July 19, 2021)

FEATURES

* Add support for automatic ACL config sync (#107)
* Add support for password protected ACL users (#106)
* Use ConfigMaps for redis.conf and aclfile (#103)

CHANGES

* Change golang module name to lowercase (#105)
* Update kind to v0.11.1 (#104)
* Add fix for CRD schema (#102)
* Place CM Helm templates under flag (#110)
* Remove shell end envvar parameters from entrypoint (#108)
* Change the binary path for the development image (#109)


## 1.0.0 (March 1, 2021)

  * Initial release

FEATURES:

* **HA Setup**: multiple nodes, each with multiple replicas
* **AZ/Rack aware**: HA node distribution able to use multiple availability zones
* **Auto-recovery**: Automatic recovery from multiple failure scenarios
* **Rolling updates**: Enables to update redis pod settings gracefully
