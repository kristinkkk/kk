class car (object):
    def __init__(self, model, passengers, color, speed):  # 里面也可以设默认值
        self.model = model  # 更改car里面的实际参数
        self.passengers = passengers
        self.color = color
        self.speed = speed

    def accelerate(self):  # class里面的方法
        self.speed = self.speed+2
        print(self.speed)


# 实例化
bmw = car('BMW', 4, 'red', 5)
ferrari = car('ferrari', 2, 'black', 10)
ford = car('FORD', 6, 'blue', 6)

bmw.accelerate()  # >>>7
print(bmw.color)  # >>>red

# 1."def __init__" is a constructor method,
# it's called automatically whenever an object is  created based on the class
# it's good to have but not compulsory

# 2.self parameter in each class function - whenever an object calls its method,
# the object itself is passed as the first argument,
# so bmw.accelerate translate into car.accelerate(bmw)
