from PIL import Image
import requests
import urllib
import time
from io import BytesIO

# -- @mady by hidekuma
# Factory pattern & PIL dependency
class Imager():
	__current_img = None
	__current_bytesio = None
	__origin_format = None
	__origin_file_size = None

	def __init__(self, *args, **kwargs):
		Imager.printt(2, '=====> Imager initializing <=====')
		if args:
			Imager.printt(4, '\targs: {}'.format(args))
		for key, value in kwargs.items():
			Imager.printt(4, '\tkwargs: {} = {}'.format(key, value))
			if key == 'url':
				self.__current_img = self.__read_img_url(value)
	@property	
	def current_img(self):
		return self.__current_img

	@property
	def current_bytesio(self):
		return self.__current_bytesio

	@property
	def origin_format(self):
		return self.__origin_format

	@staticmethod
	def printt(s, msg):
		if s == 1:
			print("\033[01;31m[%s] %s\033[00m" %(time.strftime("%H:%M:%S"),msg))
		elif s == 2:
			print("\033[01;32m[%s] %s\033[00m" %(time.strftime("%H:%M:%S"),msg))
		elif s == 3:
			print("\033[01;01m[%s] %s\033[00m" %(time.strftime("%H:%M:%S"),msg))
		elif s == 4:
			print("\033[01;34m[%s] %s\033[00m" %(time.strftime("%H:%M:%S"),msg))
		elif s == 5:
			print("\033[01;35m[%s] %s\033[00m" %(time.strftime("%H:%M:%S"),msg))
		else:
			print("\033[01;01m[%s] %s\033[00m" %(time.strftime("%H:%M:%S"),msg))
	
	@staticmethod
	def bytes_2_human_readable(number_of_bytes):
		if number_of_bytes < 0:
			Imager.printt(1, "number_of_bytes can't be smaller than 0 !")
		step_to_greater_unit = 1024.
		number_of_bytes = float(number_of_bytes)
		unit = 'bytes'
		if (number_of_bytes / step_to_greater_unit) >= 1:
			number_of_bytes /= step_to_greater_unit
			unit = 'KB'
		if (number_of_bytes / step_to_greater_unit) >= 1:
			number_of_bytes /= step_to_greater_unit
			unit = 'MB'
		if (number_of_bytes / step_to_greater_unit) >= 1:
			number_of_bytes /= step_to_greater_unit
			unit = 'GB'
		if (number_of_bytes / step_to_greater_unit) >= 1:
			number_of_bytes /= step_to_greater_unit
			unit = 'TB'
		precision = 1
		number_of_bytes = round(number_of_bytes, precision)
		return str(number_of_bytes) + ' ' + unit

	def show(self):
		self.__current_img.show()
		return self

	# URL to Image bytesio
	def __read_img_url(self, url):
		r = requests.get(url)
		#if r.status_code == 200:
		inputio = BytesIO(r.content)
		image = self.__PIL_img_open(inputio)
		return image
		
	# 1) current_bytesio update
	def __PIL_img_open(self, bytesio):
		img = Image.open(bytesio)	
		self.__current_bytesio = bytesio
		# only 1 action
		if self.__current_img is None:
			# format initialize
			self.__origin_format = img.format
			self.__origin_file_size = bytesio.getbuffer().nbytes
			Imager.printt(2, '========== origin ==========')
			Imager.printt(5, Imager.bytes_2_human_readable(self.__origin_file_size)) 
			Imager.printt(5, 'format: {}'.format(img.format))
			Imager.printt(5, 'size: {}'.format(img.size))
		return img

	# 2) current_bytesio update
	def resize(self, width, height):
		print()
		Imager.printt(2, '========== resize {} x {} =========='.format(width, height))
		self.__current_bytesio = BytesIO()
		self.__current_img.resize((width, height), Image.ANTIALIAS).save(self.__current_bytesio, format=self.__origin_format)
		#self.__current_bytesio.seek(0)
		file_size = self.__current_bytesio.getbuffer().nbytes
		self.__is_compressed(file_size)
		if self.__is_compressed(file_size):
			self.__current_img = self.__PIL_img_open(self.__current_bytesio)
		Imager.printt(5, Imager.bytes_2_human_readable(file_size))
		Imager.printt(5, 'format: {}'.format(self.__current_img.format))
		Imager.printt(5, 'size: {}'.format(self.__current_img.size))
		return self

	# 3) current_bytesio update
	def compress(self, quality, optimize=True):
		print()
		Imager.printt(2, '========== compress {} % =========='.format(quality))
		self.__current_bytesio = BytesIO()
		self.__current_img.save(self.__current_bytesio, optimize=optimize, quality=quality, format=self.__origin_format)
		#self.__current_bytesio.seek(0)
		file_size = self.__current_bytesio.getbuffer().nbytes
		if self.__is_compressed(file_size):
			self.__current_img = self.__PIL_img_open(self.__current_bytesio)
		Imager.printt(5, Imager.bytes_2_human_readable(file_size))
		Imager.printt(5, 'format: {}'.format(self.__current_img.format))
		Imager.printt(5, 'size: {}'.format(self.__current_img.size))
		return self

	def __is_compressed(self, file_size):
		if self.__origin_file_size > file_size:
			return True
		Imager.printt(1, 'is not compressed. return origin image!')
		return False
