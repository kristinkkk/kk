#1.定义class person
class Person:
    def __init__ (self, first_name,last_name):
        self.first_name=first_name
        self.last_name=last_name

    def print_name(self):
        print(self.first_name, self.last_name)

#调用person class里面的print_name函数：
x= Person('john', 'smith')
x.print_name()

print(x) #>>  <__main__.Person object at 0x104a9bfd0>
print(x.first_name, x.last_name) #>>>john smith

#2.define a child class: studnet
class Student(Person):
    pass
#child class会继承父类class的属性：
my_student = Student('kkr','zheng')
my_student.print_name() #>>>kkr zheng

#3.继承父类属性
class Student(Person):
    def __init__(self, first_name, last_name, student_id):
        Person.__init__(self, first_name, last_name)
        self.student_id=student_id

my_student_id = Student (first_name='kk',last_name='simti', student_id=123455)
print(my_student_id.first_name, my_student_id.last_name, my_student_id.student_id)
#>>>kk simti 123455

#4.
class Student(Person):
    def __init__(self, student_id):
        Person.__init__(self, first_name='k', last_name='g')
        self.student_id=student_id

my_student_id = Student (student_id=123455)
print(my_student_id.first_name, my_student_id.last_name, my_student_id.student_id)

#>>>kk simth 123455