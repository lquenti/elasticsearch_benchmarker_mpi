import json
import string
import random

from datetime import date, timedelta
from dataclasses import dataclass

import numpy as np

N = 10**6
ROUND_TO = 4

ALL_STRING_VALUES = [*string.ascii_letters, *string.digits, ' ']

# BEGIN STRING GENERATION FUNCTIONS
def gen_string_fixed_length(length):
    return ''.join(random.choices(ALL_STRING_VALUES, k=length))

def gen_string_range_length(min_length, max_length):
    return gen_string_fixed_length(random.randint(min_length, max_length))

# BEGIN RANDOM DATE GENERATION FUNCTIONS
def random_date_string(start_date, end_date):
    delta = end_date - start_date
    random_day = random.randint(0, delta.days)
    random_date = start_date + timedelta(days=random_day)
    return random_date.strftime("%Y-%m-%d")

def generate_document():
	doc = {}

	# Copied from the actual meeting logs

	# "ismrmrdHeader" : "Text()",
	# I just looked up what an ISMRMRD header is, and this is not mockable
	doc["ismrmrdHeader"] = gen_string_range_length(8, 100)

	# "cflHeader" : "Text()",
	# Coil File List also not mockable since I do not know the structure of our manufacturer's coil files
	doc["cflHeader"] = gen_string_range_length(8, 100)

	# "DataFormat" : "Text()",
	doc["DataFormat"] = random.choice(["h5", "dat", "nii", "nii.gz", "dicom"])

	# "FrameOfReferenceUID" : "Text()",
	# some uid, thus the format is not interesting
	doc["FrameOfReferenceUID"] = gen_string_fixed_length(36)

	# "MeasurementID" : "Text()",
	# big number as string?
	doc["MeasurementID"] = str(random.randint(10000000000, 99999999999))


	# "StudyID" : "Text()",
	# again but smaller?
	doc["StudyID"] = str(random.randint(10000, 99999))


	# "StudyTimeText" : "Text()",
	# TODO I really do not know what this could be
	doc["StudyTimeText"] = gen_string_range_length(8, 20)

	# "SystemVendor" : "Text()",
	# I just chose some
	system_vendors = ["GE", "Siemens", "Philips", "Canon", "Hitachi", "Bruker"]
	vendor_i = random.choice([*range(len(system_vendors))])
	doc["SystemVendor"] = system_vendors[vendor_i]

	# "SystemModel" : "Text()",
	# I chose some as well but it should be fitting to the vendor
	system_models = ["SIGNA", "MAGNETOM", "Achieva", "Vantage", "Oasis", "BioSpec"]
	doc["SystemModel"] = system_models[vendor_i]

	# "ProtocolName" : "Text()",
	# They mentioned that it was based on the manufacturer as well, so lets define 2 for each
	protocol_names = [
             ["GE1", "GE2", "GE3"],
			 ["Siemens1", "Siemens2", "Siemens3"],
			 ["Philips1", "Philips2", "Philips3"],
			 ["Canon1", "Canon2", "Canon3"],
			 ["Hitachi1", "Hitachi2", "Hitachi3"],
			 ["Bruker1", "Bruker2", "Bruker3"]
	]
	doc["ProtocolName"] = random.choice(protocol_names[vendor_i])

	# "SystemFieldStrength" : "Float()", -> 1.5, 3 , 7 (Mit Toleranz .2, aber Tol immer gleich pro Scanner)
	doc["SystemFieldStrength"] = random.choice([1.50, 3.00, 7.00])

	# "NumberReceiverChannels" : "Integer()", 8,12,16,32,64
	doc["NumberReceiverChannels"] = random.choice([8, 12, 16, 32, 64])

	# "InstitutionName" : "Text()",
	# just chose some
	# but again, we link it to the address
	institution_names = [
		"Charité - Universitätsmedizin Berlin",
		"Heidelberg University Hospital",
		"University Hospital Cologne",
		"LMU Klinikum Munich",
		"University Hospital Frankfurt",
		"UMG - Universitätsmedizin Göttingen",
		"University Hospital Hamburg-Eppendorf",
		"University Hospital Tübingen",
		"University Hospital Essen",
		"University Hospital Bonn"
	]
	institution_i = random.choice([*range(len(institution_names))])
	doc["InstitutionName"] = institution_names[institution_i]

	# "InstitutionAddress" : "Text()",
	# just chose the same as above
	institution_addresses = [
		"Charitéplatz 1, 10117 Berlin, Germany",
		"Im Neuenheimer Feld 400, 69120 Heidelberg, Germany",
		"Kerpener Str. 62, 50937 Köln, Germany",
		"Marchioninistraße 15, 81377 München, Germany",
		"Theodor-Stern-Kai 7, 60590 Frankfurt am Main, Germany",
		"Robert-Koch-Str. 40, 37075 Göttingen, Germany",
		"Martinistraße 52, 20246 Hamburg, Germany",
		"Geissweg 3, 72076 Tübingen, Germany",
		"Hufelandstraße 55, 45147 Essen, Germany",
		"Sigmund-Freud-Straße 25, 53127 Bonn, Germany"
	]
	doc["InstitutionAddress"] = random.choice(institution_addresses[institution_i])

	# "Trajectory" : "Text()", -> Sehr diskret (Cartesian, Radial, Spiral)
	doc["Trajectory"] = random.choice(["Cartesian", "Radial", "Spiral"])

	# "AcquisitionType" : "Text()", -> 3D, 2D
	doc["AcquisitionType"] = random.choice(["3D", "2D"])

	# "AccelerationFactor" : "Float()", -> 1,2,4,8
	doc["AccelerationFactor"] = random.choice([1.0, 2.0, 4.0, 8.0])

	# "TR" : "Float()", -> Range [5ms, 700ms] (halbwegs Diskret) (gehäuft GENAU dasselbe, unklar ob minimal noise drüber)
	vals_in_mm = [5, 10, 20, 30, 40, 50, 60, 70, 80, 90, 100, *range(200, 700, 100)]
	vals_in_SI = [x / 1000 for x in vals_in_mm]
	doc["TR"] = random.choice(vals_in_SI)

	# "TI" : "Float()", -> Range [200ms, 700ms] (halbwegs Diskret)
	vals_in_mm = [*range(200, 700, 100)]
	vals_in_SI = [x / 1000 for x in vals_in_mm]
	doc["TI"] = random.choice(vals_in_SI)

	# "TE" : "Float()", -> Range [1ms, 10ms] (halbwegs Diskret)
	vals_in_mm = [*range(1, 10, 1)]
	vals_in_SI = [x / 1000 for x in vals_in_mm]
	doc["TE"] = random.choice(vals_in_SI)

	# "FlipAngle" : "Float()", -> Range [3deg, 90deg]
	doc["FlipAngle"] = round(random.uniform(3, 90), ROUND_TO)

	# "SequenceType" : "Text()", -> Sehr diskret
	doc["SequenceType"] = random.choice(["GradientEcho", "SpinEcho", "InversionRecovery"])

	# "EchoSpacing" : "Float()", -> Wie TE
	doc["EchoSpacing"] = random.choice(vals_in_SI)

	# "KspaceX" : "Integer()", -> 2er potenzen 64-512 , 5*2er potenz, optimiert für FFT
	doc["KspaceX"] = random.choice([64, 128, 256, 512])

	# "KspaceFOVX" : "Float()", an Patienten und Organ angemessen -> normalverteilt [] Kop um 25 cm, Brust 40cm, also wieder geclustered samplen rund um eine Normalverteilung
	head = np.random.normal(25, 5, 20)
	chest = np.random.normal(40, 5, 20)
	KSpaceFOVX_in_SI = [round(x/100, ROUND_TO) for x in [*head, *chest]]
	doc["KspaceFOVX"] = random.choice(KSpaceFOVX_in_SI)

	# "KspaceFOVY" : "Float()", -> See FOVX
	doc["KspaceFOVY"] = random.choice(KSpaceFOVX_in_SI)

	# "KspaceFOVZ" : "Float()", -> ggf 0 bei 2D
	# 50% chance 0, 50% chance normal
	doc["KspaceFOVZ"] = random.choice([*[0 for _ in range(40)], *KSpaceFOVX_in_SI])

	# "KspaceReadout" : "Integer()", -> Wie KSpaceX
	doc["KspaceReadout"] = random.choice([64, 128, 256, 512])

	# "KspacePhaseEnc1" : "Integer()", -> Wie KSpaceX oder Random [13-500] mit Häufugspunkten
	doc["KspacePhaseEnc1"] = random.choice([64, 128, 256, 512, *range(13, 500, 25)])

	# "KspacePhaseEnc2" : "Integer()", oft 1, sonst wie kspacex or enc1
	doc["KspacePhaseEnc2"] = random.choice([*[1 for _ in range(25)], 64, 128, 256, 512, *range(13, 500, 25)])

	# "KspaceCoils" : "Integer()", -> SELBER Wert wie NumberReceiverCHannels
	doc["KspaceCoils"] = doc["NumberReceiverChannels"]

	# "KspaceMaps" : "Integer()", -> const 1
	doc["KspaceMaps"] = 1

	# "KspaceTE" : "Integer()", -> const 1
	doc["KspaceTE"] = 1

	# "KspaceCoeff" : "Integer()", -> const 1
	doc["KspaceCoeff"] = 1

	# "KspaceCoeff2" : "Integer()", -> const 1
	doc["KspaceCoeff2"] = 1

	# "KspaceIter" : "Integer()", -> const 1
	doc["KspaceIter"] = 1

	# "KspaceCshift" : "Integer()", -> const 1
	doc["KspaceCshift"] = 1

	# "KspaceTime" : "Integer()", -> const 1
	doc["KspaceTime"] = 1

	# "KspaceTime2" : "Integer()", -> const 1
	doc["KspaceTime2"] = 1

	# "KspaceLevel" : "Integer()", -> const 1
	doc["KspaceLevel"] = 1

	# "KspaceSlice" : "Integer()", 1 oder uniform verteilt [80,200]
	doc["KspaceSlice"] = random.choice([*[1 for _ in range(25)], *range(80, 200, 5)])

	# "KspaceAvg" : "Integer()", -> const 1?
	doc["KspaceAvg"] = 1

	# "ImageFOVX" : "Float()", -> Exact wie KSpaceFOV
	doc["ImageFOVX"] = doc["KspaceFOVX"]

	# "ImageFOVY" : "Float()", -> Exact wie KSpaceFOV
	doc["ImageFOVY"] = doc["KspaceFOVY"]

	# "ImageFOVZ" : "Float()", -> Exact wie KSpaceFOV
	doc["ImageFOVZ"] = doc["KspaceFOVZ"]

	# "ImageMatrixX" : "Integer()", -> Häufig kspacereadout oder (/2), Enc1 sonst kspacex
	doc["ImageMatrixX"] = random.choice([doc["KspaceReadout"], doc["KspaceReadout"]//2, doc["KspacePhaseEnc1"], doc["KspaceX"]])

	# "BaseResolution": "Integer()", ImageFOVX / ImageMatrixX
	doc["BaseResolution"] = int(doc["ImageFOVX"] / doc["ImageMatrixX"])

	# "ImageMatrixY" : "Integer()", -> Enc2
	doc["ImageMatrixY"] = doc["KspacePhaseEnc2"]

	# "ImageMatrixZ" : "Integer()",
	# No idea, just guessing
	doc["ImageMatrixZ"] = random.randint(1, 100)

	# "SliceThickness" : "Float()", 1mm , 0.8mm, 1.2mm bis 6mm (ggf einfach uniform samplen) -> Diskret verteilt | oder *KspacePhaseEnc2
	vals_in_mm = [1, 0.8, 1.2, 1.5, 2, 3, 4, 5, 6]
	vals_in_SI = [x/1000 for x in vals_in_mm]
	doc["SliceThickness"] = random.choice([*[x*doc["KspacePhaseEnc2"] for x in vals_in_SI], *vals_in_SI])

	# "BodyPart" : "Keyword()", -> Diskret Herz, Knie, Kopf
	doc["BodyPart"] = random.choice(["Heart", "Knee", "Head"])

	# "PatientID": "Keyword()",
	# some long id?
	# but not too many options so that we have some duplicates
	doc["PatientID"] = str(random.randint(1000000000, 1000002000))

	# "PatientName": "Keyword",
	# I just put some german last names here
	doc["PatientName"] = random.choice(["Müller", "Schmidt", "Schneider", "Fischer", "Weber", "Meyer", "Wagner", "Becker", "Schulz", "Hoffmann"])

	# "PatientBirthday": "Date()",
	doc["PatientBirthday"] = random_date_string(date(1900, 1, 1), date(2000, 1, 1))

	# "PatientWeight" : "Float()",
	doc["PatientWeight"] = round(random.uniform(50, 150), ROUND_TO)

	# "PatientSex" : "Text()",
	doc["PatientSex"] = random.choice(["M", "F"])

	# "PatientAge" : "Integer()",
	doc["PatientAge"] = random.randint(6, 100)

	# "StudyDate" : "Date()",
	doc["StudyDate"] = random_date_string(date(2010, 1, 1), date(2020, 1, 1))

	# "Lamor_frequency": "Float()", -> prop zum SystemFieldStrength
	doc["Lamor_frequency"] = doc["SystemFieldStrength"] * 42

	# "TNumber" : "Text()", -> T{inc_zwischen(0,100000)}, uniform
	doc["TNumber"] = f"T{random.randint(0, 100000)}"

	# "Tags" : "Keyword(multi=True)"
	doc["Tags"] = random.sample(["Tag1", "Tag2", "Tag3", "Tag4", "Tag5", "Tag6", "Tag7", "Tag8", "Tag9", "Tag10"], random.randint(0, 10))

    # After further query sketching (2023-04-25), we discussed to add the following tags:
	# "Filename" : "Text()",
	doc["Filename"] = f"{doc['StudyDate']}_{doc['MeasurementID']}{doc['DataFormat']}"

	# "BrainAge": "Integer()",
	# of couse, this would normally not be in the unprocessed data, but we need it for the query
	# it is the PatientAge + a normally distributed random number with mean 0 and std 5
	doc["BrainAge"] = max(1, doc["PatientAge"] + round(random.normalvariate(0, 5)))

	return json.dumps(doc)



if __name__ == '__main__':
    with open('data.json', 'w') as f:
        for i in range(N):
            if i % 10000 == 0:
                print(f"Progress: {i}/{N}")
            f.write(generate_document())
            f.write("\n")
