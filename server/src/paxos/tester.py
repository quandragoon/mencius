#!/usr/bin/env python

import subprocess

for i in range(100):
	p = subprocess.Popen('go test' , stdout = subprocess.PIPE, stderr = subprocess.PIPE, shell = True)
	out, err = p.communicate()
	if 'PASS' in out:
		print i, '----PASSED----'
	else:
		print i, '----FAILED----'
		print out
