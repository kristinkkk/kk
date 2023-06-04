from Base_task import BaseTask

class test(BaseTask):
    def __init__(self):
        super().__init__(task_name='test')

my_class=test()
print(my_class.task_status)