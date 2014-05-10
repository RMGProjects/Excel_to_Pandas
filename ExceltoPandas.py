import pandas as pd
import numpy as np
from pandas import DataFrame
from numpy import nan as NA
import random, datetime, json, os, pickle
from WorkbookFunctions import _InputError, _NotFoundError
	
class workbook_iterator(object):
	def __init__(self, wkbk_file_path, line_codes, workbook_structure):
		"""
		wkbk_file_path 		: string
		line_codes 			: DataFrame
		workbook_structure 	: dict
		
		SuperClass for creating workbook_iterator object/n
		
		Attributes
		All_DFs		: dict of all DataFrames in ExcelFile from wkbk_file_path
		line_codes	: DataFrame of line code information as per line_codes argument
		line_vals	: list of 'line' values from line_codes
		DFs			: list of names of DataFrames in ExcelFile from wkbk_file_path
		"""		
		self.__wkbk_DF = pd.ExcelFile(wkbk_file_path) #private to SuperClass
		self._wkbk_struc = workbook_structure
		self.line_codes = line_codes
		self.line_vals = [line for line in self.line_codes['line']]
		self.All_DFs = {sheet : DataFrame() for sheet in self.__wkbk_DF.sheet_names}
		for sheet in self.All_DFs.keys():
			len_DF = len(self.__wkbk_DF.parse(sheet))
			DF = self.__wkbk_DF.parse(sheet,
									  header = self._wkbk_struc['start_rows'][sheet],
									  skip_footer = len_DF - self._wkbk_struc['end_rows'][sheet],
									  parse_cols = self._wkbk_struc['cols']
									 )
			DF['Index1'] = pd.Series([sheet for x in xrange(len(DF))])
			DF['Index2'] = pd.Series([i for i in xrange(len(DF))])
			DF.set_index(['Index1', 'Index2'], inplace = True)
			self.All_DFs[sheet] = DF
		self.DFs = self.All_DFs.keys()
		self.DFs.sort()
		
	def _numerical_lines(self, DF_Series):
		"""
		DF_Series	: Series or other iterable
		return		: list
		method		: hidden
		
		Helper function that is called in subsequent function. Returns list of
		elements that are strings where numerical values in DF_Series are 
		cast to type int before being cast to string.
		"""
		DF_line_vals = []
		for line in DF_Series:
			try:
				DF_line_vals.append(str(int(line)))
			except ValueError:
				DF_line_vals.append(str(line))
		DF_line_vals = [line_val if line_val != 'nan' else NA for line_val in DF_line_vals]
		return DF_line_vals

class workbook_checker(workbook_iterator):
	def __init__(self, wkbk_file_path, line_codes, workbook_structure):
		"""
		wkbk_file_path 		: string
		line_codes 			: DataFrame
		workbook_structure 	: dict
		
		Subclass of workbook_iterator for checking primarily whether the lines in 
		the line_codes DataFrame appear consistently in each DataFrame that is 
		part of the workbook_iterator.All_DFs attribute.\n
		This subclass adds the attribute col_names which is a list of the 
		columns names found in the first DataFrame in the All_DFs attribute.
		"""
		super(workbook_checker, self).__init__(wkbk_file_path, line_codes, workbook_structure)
		self.col_names = list(self.All_DFs[self.All_DFs.keys()[0]].columns)	
	
	def check_single_lines(self, line_col_ref, numerical = True):
		"""
		line_col_ref 	: string
		numerical 		: bool
		return			: dict
		method			: visible
		
		Function returns dict of values that has keys that are DataFrame names
		as per self.All_DFs, and values that are lists of line numbers/values
		that are present in self.line_vals but not in the DataFrame 
		column referenced by line_col_ref.\n
		If numerical = True then the _numerical_lines function is called to ensure
		that the numerical values taken from the DataFrame in question are 
		strings of integers (as they are in self.line_vals
		"""
		missing_lines = {DF : [] for DF in self.DFs}
		for DF in self.DFs:
			if numerical:
				DF_line_vals = self._numerical_lines(self.All_DFs[DF][line_col_ref])
			else:
				DF_line_vals = list(self.All_DFs[DF][line_col_ref])
			if len(set(DF_line_vals).intersection(self.line_vals)) != len(self.line_vals):
				missing_lines[DF].extend(list(set(self.line_vals).difference(DF_line_vals)))
		
		missing_lines = {key : value for key, value in missing_lines.iteritems() if value}
		return missing_lines
		
	def check_multiple_lines(self, line_col_ref, numerical = True):
		"""
		line_col_ref 	: string
		numerical 		: bool
		return			: dict
		method			: visible
		
		Function returns dict of values that has keys that are DataFrame names
		as per self.All_DFs, and values that are lists of line numbers/values
		that are present in self.line_vals and multiple (more than once) in the 
		DataFrame column referenced by line_col_ref.\n
		If numerical = True then the _numerical_lines function is called to ensure
		that the numerical values taken from the DataFrame in question are 
		strings of integers (as they are in self.line_vals).
		"""		
		multiple_lines = {DF : [] for DF in self.DFs}
		for DF in self.DFs:
			if numerical:
				DF_line_vals = self._numerical_lines(self.All_DFs[DF][line_col_ref])
			else:
				DF_line_vals = list(self.All_DFs[DF][line_col_ref])
			for line in self.line_vals:
				if DF_line_vals.count(line) > 1:
					multiple_lines[DF].append(line)
		multiple_lines = {key : value for key, value in multiple_lines.iteritems() if value}
		return multiple_lines
		
	def check_unusual_lines(self, line_col_ref, other_lines, numerical = True)
		"""
		line_col_ref 	: string
		other_lines 	: list
		numerical 		: bool
		return			: dict
		method			: visible
		
		Function returns dict of values that has keys that are DataFrame names
		as per self.All_DFs, and values that are lists of line names/numbers
		that are present in the DataFrame, but are not present in either 
		self.line_vals or the list of other_lines. The line numbers in the 
		DataFrames are identified as being in the column referenced by line_col_ref.
		
		If numerical = True then the _numerical_lines function is called to ensure
		that the numerical values taken from the DataFrame in question are 
		strings of integers (as they are in self.line_vals).
		"""		
		
		unusual_lines_dict = {DF : [] for DF in self.DFs}
		total_lines = self.line_vals[:]
		total_lines.extend([str(val) for val in other_lines])
		for DF in self.DFs:
			if numerical:
				DF_line_vals = self._numerical_lines(self.All_DFs[DF][line_col_ref])
				DF_line_vals = [val for val in DF_line_vals if not pd.isnull(val)] 
			else:
				DF_line_vals = list(self.All_DFs[DF][line_col_ref])
				DF_line_vals = [val for val in DF_line_vals if not pd.isnull(val)]
			unusuals = set(DF_line_vals).difference(total_lines)
			if len(unusuals) > 0:
				unusual_lines_dict[DF].extend([unusual for unusual in unusuals])
		unusual_lines = {key : value for key, value in unusual_lines.iteritems() if value}
		return unusual_lines
	
class workbook_concatenator(workbook_iterator):
	def __init__(self, wkbk_file_path, line_codes, workbook_structure, line_col_ref, numerical = True, merge = True):
		"""
		wkbk_file_path 		: string
		line_codes 			: DataFrame
		workbook_structure 	: dict
		line_col_ref		: string
		numerical			: boolean
		merge				: boolean
		
		Subclass of workbook_iterator for (optionally) merging line code data with 
		data in rhe DataFrames contained in the All_DFs attribute and concatenating.\n
		If you believe the values in the line_col_ref to be numerical then leave
		numerical as True. This will replace these values with strings, so that
		the can be merged with the line_code data which is imported as a string.\n
		If merge is True then the data in the All_DFs will be merged with the 
		line_code data on the line_col_ref in a 'left' merge. If it is thought that
		this is not appropriate then set merge to False.\n		
		This subclass adds the attribute All_DFs_merged if merge is True then this
		will include the line_code information and a date as per the 
		workbook_structure. If merge is False it will only add the date.  
		"""
		super(workbook_concatenator, self).__init__(wkbk_file_path, line_codes, workbook_structure)
		self.All_DFs_merged = self.All_DFs.copy()
		for DF in self.DFs:
			DFM = self.All_DFs[DF]
			if numerical:
				line_Series = pd.Series(self._numerical_lines(DFM[line_col_ref]), index = DFM.index)
				DFM[line_col_ref] = line_Series.astype(str)
			if merge:
				DFM.reset_index(inplace = True)
				DFM = DFM.merge(self.line_codes, 
								left_on = line_col_ref,
								right_on = 'line',
								how = 'left')
				DFM.set_index(['Index1', 'Index2'], inplace = True)
			DFM['date'] = self._wkbk_struc['dates'][DF]
			self.All_DFs_merged[DF] = DFM
			
	def concat_all(self, drop_na_lines = False):
		"""
		drop_na_lines : bool
		return		  : DataFrame
		
		Method concatenates the DataFrames stored in All_DFs. If drop_na_lines is
		False then no rows are dropped. If it is True then all rows with a null value
		of 'line_code' will be dropped. This option is only available if the 
		object creator has been passed merge == True. If this was not the case
		then drop_na_lines must be False. Only drop lines if you are sure that the
		data contain only one observation per line/day.
		"""
		DFC = pd.concat(self.All_DFs_merged[DF] for DF in self.DFs)
		if drop_na_lines:
			DFC = DFC[pd.notnull(DFC['line_code'])]
		return DFC
		
def get_workbook_stucture(json_file_path):
	"""
	json_file_path	: string
	returns			: json data (dict)
	
	Function returns a dict if the json_file_path provided points to the 
	workbook_structure dict created by the WorkbookFunctions module processes. The
	function will however open any general .json file"""
	json_file = json_file_path
	json_data = open(json_file)
	workbook_structure = json.load(json_data)
	json_data.close()
	return workbook_structure
	
def get_line_codes(codes_file_path):
	"""
	codes_file_path	: string
	returns			: DataFrame
	
	Functions returns data frame drawn from the csv file identified by 
	codes_file_path.\n
	Only the first five columns of the csv file are retained, and column names
	are standardised.\n
	An InputError is raised if there are any missing values in the resulting
	DataFrame.
	"""
	cols = [0, 1, 2, 3, 4]
	col_names = ['fact_code', 'unit_floor',	'line', 'line_code', 's_line']
	line_codes = pd.read_csv(codes_file_path, 
						     header = 0,
							 usecols = [0, 1, 2, 3, 4],
					         names = col_names,
							 dtype = str)
	for column in line_codes.columns:
		if list(line_codes[column].isnull()).count(True) != 0:
			raise _InputError("Missing values in cvs file")						 
	return line_codes
			
def pickle_dataframe(DF, top_folderpath, name):
	"""
	DF				: DataFrame
	top_folderpath	: string
	name			: string
	return			: string
	
	Function pickles DF in top_folderpath according to name, returns sting message
	if successful. 
	"""
	os.chdir(top_folderpath)
	pickle.dump(DF, open(name, 'wb'))
	return 'Object Pickled Sucessfully'
	
def unpickle_dataframe(top_folderpath, file_name):
	"""
	top_folderpath	: string
	file_name		: string
	return 			: Unpickled Object
	
	Function unpickles file_name from top_folderpath abd returns the result.
	"""
	os.chdir(top_folderpath)
	DF = pickle.load(open(file_name, "rb"))
	return DF
	