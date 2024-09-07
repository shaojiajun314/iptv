class RetVideo():
	def __init__(
		self,
		title,
		category,
		area,
		year,
		key_m3u8_map,
		platform
	):

		self.title = title
		self.category = category
		self.area = area
		self.year = year
		self.key_m3u8_map = key_m3u8_map
		self.platform = platform

		assert all([
			isinstance(i, dict) and all([
				isinstance(j, str)
				for j in i.values()
			]) 
			for i in key_m3u8_map.values()
		])

	@property
	def tag(self):
		return f'<RetVideo title={self.title}, category={self.category}, area={self.area}, year={self.year}, platform={self.platform}, key_m3u8_map={self.key_m3u8_map}>'

	def __str__(self):
		return self.tag

	def __repr__(self):
		return self.tag