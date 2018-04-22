import yaml
import conf.config as config

# Class for Reading and Writing YAML Config files for the rescaler
class ConfigWriter:

    def writeConfig(self,metrics,src_file="config.yaml",filename="out.yaml", decision="up",overall_para=0):
        stream = self.readConfig(src_file)


        if decision == "out":
            stream["out"] = "out"
        else:
            stream["out"] = "up"

        if overall_para > 0:
            stream["parallelism"] = overall_para

        for key in metrics.keys():
            stream[key] = metrics[key].get_parallelism()

        stream["restarted"] = "true"

        outfile = open(filename,"w")
        yaml.dump(stream,outfile,default_flow_style=False)


    def readConfig(self,src_file="config.yaml"):
        stream = open(src_file, "r")
        return yaml.load(stream)


