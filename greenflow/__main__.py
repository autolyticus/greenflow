import sys
import fire
import gin
from icecream import install

from . import deploy, destroy, run

from sh import ssh, helm
from shlex import split
from os import system

print(sys.argv)


class RUN:
    def upto(self):
        gin.parse_config_file("params/1worker-1resources-kafka.gin")
        deploy.deploy()
        run.base()
        run.kafka()
        run.theo()

    def exp(self):
        gin.parse_config_file("params/1worker-1resources-kafka.gin")
        run.exp()

    def theo(self):
        gin.parse_config_file("params/1worker-1resources-kafka.gin")
        run.theo()

    def kafka(self):
        run.kafka()

    def deploy(self):
        gin.parse_config_file("params/1worker-1resources-kafka.gin")
        return deploy.deploy()

    def destroy(self):
        gin.parse_config_file("params/1worker-1resources-kafka.gin")
        return destroy.destroy()

    def mock_destroy(self):
        return destroy.mock_destroy()

    def blowaway(self):
        gin.parse_config_file("params/1worker-1resources-kafka.gin")
        return destroy.blowaway()

    def killjob(self):
        gin.parse_config_file("params/1worker-1resources-kafka.gin")
        destroy.mock_destroy()
        destroy.killjob()

    def base(self):
        run.base()

    def mrun(self):
        run.mrun()

    def full(self, ginfile):
        gin.parse_config_file(ginfile)
        deploy.deploy()
        run.base()
        run.kafka()
        run.theo()
        run.exp()

    def tight(self):
        gin.parse_config_file("params/1worker-1resources-kafka.gin")
        # deploy.deploy()
        # run.base()
        # run.vm()
        # run.vm()
        # run.kafka()
        # run.theo()
        run.exp()
        c = input("Press any key to mock_destroy or q to exit")
        if c == "q":
            return
        destroy.mock_destroy()
        helm(split("uninstall theodolite"))
        # helm(split("uninstall strimzi"))

    def e2e(self):
        gin.parse_config_file("params/1worker-1resources-kafka.gin")
        # gin.parse_config_file(ginfile)
        deploy.deploy()
        run.base()
        run.vm()
        run.kafka()
        run.theo()
        run.exp()
        # c = input("Press any key to mock_destroy or q to exit")
        # if c == "q":
        #     return
        # destroy.mock_destroy()
        # c = input("Press any key to vm or q to exit")
        # if c == "q":
        #     return
        destroy.destroy()
        # run.vm()
        # destroy.blowaway()

    def vm(self):
        ginfile = "params/1worker-1resources-kafka.gin"
        gin.parse_config_file(ginfile)
        # deploy.deploy()
        # run.base()
        run.vm()

    def redpanda(self):
        ginfile = "params/1worker-3resources-redpanda.gin"
        gin.parse_config_file(ginfile)
        # deploy.deploy()
        # run.base()
        run.redpanda()
        # run.theo()
        # run.exp()
        # destroy.destroy()

    def mock(self):
        gin.parse_config_file("params/mock.gin")
        deploy.deploy()
        destroy.destroy()

    def mdeploy(self):
        gin.parse_config_file("params/mock.gin")
        deploy.deploy()

    def sync(self):
        system(
            "ssh -t h-0 sudo rsync -aXxvPh --exclude '*cache*' --exclude '*tmp*' --exclude '*txn*' --exclude '*lock*' --info=progress2 /mnt/energystream1/ /root/energystream1-mirror"
        )
        ssh(split("h-0 docker restart greenflow-vm-1"))


if __name__ == "__main__":
    install()
    gin.parse_config_file("params/1worker-1resources-kafka.gin")
    fire.Fire(RUN)
