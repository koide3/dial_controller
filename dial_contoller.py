#!/usr/bin/python3
import time
import numpy
import rclpy
from rclpy.node import Node
from geometry_msgs.msg import Twist

import paho.mqtt.client as mqtt

class DialController(Node):
  def __init__(self):
    super().__init__('dial_controller')
    
    self.max_vel = numpy.array([1.0, 1.0])                      # w, v
    self.vel_per_count = numpy.array([0.1 / 2.0, 1.0 / 100.0])  # w, v
    
    self.rotary_queue = []
    self.setup_mqtt()

    self.cmd_vel_pub = self.create_publisher(Twist, 'cmd_vel', 10)
    self.timer = self.create_timer(0.1, self.timer_callback)
    self.pub_timer = self.create_timer(2.0, self.pub_timer_callback)
  
  def setup_mqtt(self):
    def on_connect(client, userdata, flags, rc):
      if rc == 0:
        print('Connected to broker')
      else:
        print('Connection failed', rc)
        return
      
      client.subscribe('/dial/rotary')
      client.subscribe('/dial/button')
    
    self.client = mqtt.Client('receiver')
    self.client.username_pw_set('dial', 'dialdial')
    
    self.client.on_connect = on_connect
    self.client.on_message = self.on_mqqt_message
    self.client.connect('localhost', 1883)
    self.client.loop_start()

  def on_mqqt_message(self, client, userdata, msg):
    if msg.topic is None or msg.payload is None:
      print('error: invalid message')
      return
    
    if msg.topic == '/dial/rotary':
      try:
        tokens = msg.payload.decode('utf-8').split(',')
        if len(tokens) != 3:
          raise ValueError('invalid message')

        tokens = [int(x) for x in tokens]
        self.rotary_queue.append(numpy.array([time.time()] + tokens))
      except:
        print('error: invalid rotary message {}'.format(tokens))
        return

    elif msg.topic == '/dial/button':
      print('button: {}'.format(msg.payload.decode('utf-8')))
    
  def timer_callback(self):
    window = 0.5
    system_now = time.time()
    device_now = self.rotary_queue[-1][1] if len(self.rotary_queue) > 0 else 0.0
    self.rotary_queue = [x for x in self.rotary_queue if system_now - x[0] <= window and device_now - x[1] <= window * 1000.0]
    
    if len(self.rotary_queue) < 2:
      self.cmd_vel_pub.publish(Twist())
      return
    
    delta = self.rotary_queue[-1] - self.rotary_queue[0]
    dt = max(delta[1] / 1000.0, 1e-3)
    raw_vel = (delta[2:] / dt) * self.vel_per_count
    vel_ = numpy.minimum(self.max_vel, numpy.maximum(-self.max_vel, raw_vel))

    sum_weights = 0.0
    sum_vels = numpy.zeros(2)   
    for prev, current in zip(self.rotary_queue[:-1], self.rotary_queue[1:]):
      weight = max(1.0 - (device_now - current[1]) / (window * 1000.0), 1e-3)
      sum_weights += weight

      dt = max(current[1] - prev[1], 1e-3) / 1000.0
      raw_vel = (current[2:] - prev[2:]) / dt * self.vel_per_count
      vel = numpy.minimum(self.max_vel, numpy.maximum(-self.max_vel, raw_vel))
      sum_vels += vel * weight
  
    sum_weights = max(sum_weights, 1e-3)
    vel = sum_vels / sum_weights
       
    msg = Twist()
    msg.linear.x = vel[1]
    msg.angular.z = vel[0]
    self.cmd_vel_pub.publish(msg)
  
  def pub_timer_callback(self):
    self.client.publish('/dial/connected', '1')
    

def main():
  rclpy.init(args=None)
  controller = DialController()
  rclpy.spin(controller)
    

if __name__ == '__main__':
  main()