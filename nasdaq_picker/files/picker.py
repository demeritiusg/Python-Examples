from picker_main import NasdaqPicker
from picker_read_text import ReadFile

np = NasdaqPicker()
rp = ReadFile()

#np.getEconData("FRED/GDP", "2001-12-31", "2005-12-31")

print(rp.read_file())