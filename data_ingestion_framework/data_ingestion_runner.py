# from data_ingestion import  DataIngestionTask
from data_ingestion.data_ingestion import DataIngestionTask


def execute():
    task = DataIngestionTask("DataIngestionTask")
    task.run() 
    #run function是从DataIngestionTask的母class - base task中继承来的，
    #实际上就是执行DataIngestionTask中的main function

#如果想用command line的方式跑代码，就需要有这个
if __name__ == "__main__":
    execute()

# test CICD in code pipeline