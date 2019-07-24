import boto3
from botocore.errorfactory import ClientError
from . import aws
from functools import wraps
from pprint import pprint
import re
import json
import base64


class S3ManagerCore:
	service_name = 's3'
	type = None
	runnable = False
	aws_access_key_id = None
	aws_secret_access_key = None
	bucket_name = None
	# region_name = None
	project_prefix = None
	project_url = None
	s3 = None
	max_item = 50
	page_size = 10

	def is_runnable(func):
		@wraps(func)
		def wrapper(self, *args, **kwargs):
			if self.runnable:
				return func(self, *args, **kwargs)
			else:
				print('is not runnable')

		return wrapper

	def __init__(self, *args, **kwargs):
		for key, value in kwargs.items():
			if key == 'type':
				self.__set_type(value)
		self.__run()

	def __set_type(self, type):
		self.type = type
		if type == "fm":
			self.max_item = 50
			self.page_size = 10
			self.aws_access_key_id = aws.AD_S3_FULL_KEY_ID
			self.aws_secret_access_key = aws.AD_S3_FULL_ACCESS_KEY
			self.bucket_name = aws.PROJECT_FILE_MANAGER_BUCKET
			# self.region_name = aws.AD_S3_REGION_NAME
			self.project_prefix = aws.PROJECT_FILE_MANAGER_PREFIX
			self.project_url = aws.PROJECT_FILE_MANAGER_URL
			self.runnable = True
		elif type == "prd":
			self.max_item = 50
			self.page_size = 10
			self.aws_access_key_id = aws.AD_S3_FULL_KEY_ID
			self.aws_secret_access_key = aws.AD_S3_FULL_ACCESS_KEY
			self.bucket_name = aws.PROJECT_AD_PRDS_BUCKET
			self.project_prefix = aws.PROJECT_AD_PRDS_PREFIX
			self.project_url = aws.PROJECT_AD_PRDS_URL
			self.runnable = True
		else:
			pass

	@is_runnable
	def __run(self):
		self.s3 = boto3.client(
			service_name=self.service_name,
			aws_access_key_id=self.aws_access_key_id,
			aws_secret_access_key=self.aws_secret_access_key,
			# region_name=self.region_name
		)

	def __is_exist(self, key):
		try:
			self.s3.head_object(Bucket=self.bucket_name, Key=key)
			return True
		except ClientError:
			return False



	@is_runnable
	def get_all_buckets(self):
		response = self.s3.list_buckets()
		buckets = [bucket['Name'] for bucket in response['Buckets']]
		print("Bucket List: %s" % buckets)
		return buckets

	@is_runnable
	def list_up_file(self, shop, group=None, token=None, type=None):
		# listup시 서치 기준 > 프로젝트 프리픽스/샵/
		# 샵 앞뒤에 / 를 넣어줌으로 특정업체만 선정한다
		# 예) 뒤에 슬러시를 안붙일경우 프리픽스/test == 프리픽스/test2
		# 전자가 후자를 포함해서 리턴해줌.
		prefix_key = self.get_path(shop=shop, group=group, filename=None, full_url_flag=False, type=type)
		paginator = self.s3.get_paginator("list_objects_v2")
		page_iterator = paginator.paginate(
			Bucket=self.bucket_name,
			Prefix=prefix_key,
			StartAfter=prefix_key,
			# Delimiter='/',
			PaginationConfig={
				'MaxItems': self.max_item,
				'PageSize': self.page_size,
				'StartingToken': token
			}, )
		# filtered_iterator = page_iterator.search("Contents[?Size > int(0)][]")
		filtered_iterator = page_iterator
		bucket_object_list = []
		for page in filtered_iterator:
			# pprint(page)
			data = {}
			if "Contents" in page:
				data = {
					'contents': [],
					'next_page': page['IsTruncated']
				}
				for c in page["Contents"]:
					# pprint(c)
					if 'Key' in c:
						info = {}
						tagging = self.__get_tags(c['Key'])
						info['url'] = self.__get_full_url(c['Key'])
						info['key'] = c['Key']
						info['key_base64'] = base64.b64encode(c['Key'].encode('utf-8')).decode('utf-8')
						info['size'] = c['Size']
						info['tag_set'] = tagging['TagSet']
						data['contents'].append(info)

				if page['IsTruncated']:
					data['next_token'] = page['NextContinuationToken']
			if bool(data):
				bucket_object_list.append(data)
		# print(bucket_object_list)
		return bucket_object_list

	def __trim(self, string):
		pattern = re.compile(r'\s+')
		return re.sub(pattern, '', string)

	def __get_full_url(self, file_key):
		return self.__trim('{}/{}'.format(self.project_url, file_key))

	def __get_tags(self, key):
		tagging = self.s3.get_object_tagging(
			Bucket=self.bucket_name,
			Key=key
		)
		return tagging

	@is_runnable
	def put_tags(self, key, tag_key):
		if not self.__is_exist(key):
			return 400
		else:
			try:
				tagging = self.__get_tags(key)
				tagset = []
				# TagSet의 Key와 Value에는 오직 문자열만 삽입이 가능함.
				# 특수기호도 안됨. 따라서 벨류에는 스트링 숫자를 삽입함.

				if len(tagging['TagSet']) > 0:
					flag = False
					for tag in tagging['TagSet']:
						if tag['Key'] == tag_key:
							flag = True
							tagset.append({
								'Key': tag_key,
								'Value': str(int(tag['Value']) + 1)
							})
						else:
							tagset.append({
								'Key': tag['Key'],
								'Value': tag['Value']
							})
					if not flag:
						tagset.append({
							'Key': tag_key,
							'Value': '1'
						})
					print(tagset)
				else:
					tagset.append({
						'Key': tag_key,
						'Value': '1'
					})
				self.s3.put_object_tagging(
					Bucket=self.bucket_name,
					Key=key,
					Tagging={
						'TagSet': tagset
					}
				)
				return 200
			except Exception as e:
				return 400

	@is_runnable
	def get_path(self, shop, group, filename, full_url_flag=False, type=None):
		# 플래그가 트루면 키만 반환함.
		# 펄스일경우엔 풀 유알엘 반환.(그룹이 있나 없나도 중요)
		if filename is None:
			filename = ''
		if type is None or type == '':
			if group is None or group == '':
				path = '{}/{}/{}'.format(self.project_prefix, shop, filename)
			else:
				path = '{}/{}/{}/{}'.format(self.project_prefix, shop, group, filename)
		else:
			if group is None or group == '':
				path = '{}/{}/{}/{}'.format(self.project_prefix, shop, type, filename)
			else:
				path = '{}/{}/{}/{}/{}'.format(self.project_prefix, shop, type, group, filename)
		if full_url_flag:
			return self.__get_full_url(path)
		else:
			return self.__trim(path)

	# 1) 업로드 파일을 위한 확장자 체크 (앞단 app에서 이미 예외처리를 하였으나 혹시 몰라 한번더)
	def __allowed_file(self, filename):
		if self.type == 'fm' or self.type == 'prd':
			return '.' in filename and \
				   filename.rsplit('.', 1)[1].lower() in aws.PROJECT_FILE_MANAGER_ALLOWED_EXTENSIONS

	# 2) 멀티파트 업로드
	def __upload_file_obj(self, file, file_key):
		self.s3.upload_fileobj(file, self.bucket_name, file_key, ExtraArgs={'ACL': 'public-read-write'})
		return self.__get_full_url(file_key)

	# 3) 파일업로드
	def __upload_file(self, origin_path, file_key):
		self.s3.upload_file(origin_path, self.bucket_name, file_key, ExtraArgs={'ACL': 'public-read-write'})
		return self.__get_full_url(file_key)
		
	@is_runnable
	def upload_bytes(self, bytesio, file_key, filename):
		if bytesio and self.__allowed_file(filename):
			self.s3.put_object(ACL='public-read-write', Body=bytesio, Bucket=self.bucket_name, Key=file_key)
		else:
			return 400
		return 200

	# 다이렉트로 S3에 저장한다.
	@is_runnable
	def upload_file_directly(self, file, file_key, filename):
		# 파일네임을 받는 이유는 확장자 체크를 한번더 하기위함.
		if file and self.__allowed_file(filename):
			if not self.__is_exist(file_key):
				self.__upload_file_obj(file, file_key)
			else:
				return 402
		else:
			return 400
		return 200

	@is_runnable
	def upload_file_directly_no_validation(self, file, file_key, filename):
		# 파일네임을 받는 이유는 확장자 체크를 한번더 하기위함.
		if file and self.__allowed_file(filename):
			self.__upload_file_obj(file, file_key)
		else:
			return 400
		return 200

	@is_runnable
	def is_owner(self, shop, file_key):
		pattern = re.compile('{}/{}/'.format(self.project_prefix, shop))
		m = pattern.match(file_key)
		if m:
			return True
		else:
			return False

	def delete_file(self, file_key):
		self.s3.delete_object(
			Bucket=self.bucket_name,
			Key=file_key
		)

	@is_runnable
	def get_info(self, shop):
		prefix = '{}/{}/'.format(self.project_prefix, shop)
		list = self.s3.list_objects_v2(
			Bucket=self.bucket_name,
			Prefix=prefix
		)
		total_size = 0
		total_bytes = 0
		groups = []
		if 'Contents' in list:
			# total_bytes = sum(_['Size'] for _ in list['Contents'])
			for _ in list['Contents']:
				if _['Size'] > 0:
					# print(_['Key'])
					g = _['Key'].replace(prefix, '').split('/')[0]
					if g not in groups:
						groups.append(g)
					total_bytes += _['Size']
					total_size += 1
		# if 'KeyCount' in list:
		#	  total_size = list['KeyCount']

		fm_type = 'limited'
		if self.is_unlimited_user(shop):
			fm_type = 'unlimited'

		# print(self.is_unlimited_user(shop))
		return {
			'type': fm_type,
			'size': total_size,
			'max_size': self.max_item,
			'bytes': total_bytes,
			'groups': groups
		}

	def is_unlimited_user(self, shop):
		if int(shop) in [74]:
			return True
		return False

	@is_runnable
	def get_html(self, key):
		response = self.s3.get_object(Bucket=self.bucket_name, Key=key)
		email_content = ''
		if 'Body' in response:
			lines = response['Body'].read().split(b'\n')
			for r in lines:
				email_content = email_content + '\n' + r.decode()
		return str(email_content)
