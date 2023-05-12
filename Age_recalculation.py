import csv
import pandas as pd
import numpy as np


class AgeRecalculation:
    def __init__(self, phase1_data, phase2_data):
        self.phase1_data = phase1_data
        self.phase2_data = phase2_data
        #self.site = site

    def AgePhase1(self, phase1_data, phase2_data):
        # convert the dates
        phase1_data['enrolment_date'] = pd.to_datetime(phase1_data.enrolment_date)
        phase2_data['gene_enrolment_date'] = pd.to_datetime(phase2_data.gene_enrolment_date)
        phase2_data['demo_dob_new'] = pd.to_datetime(phase2_data.demo_dob_new)
        phase2_data['demo_approx_dob_new'] = pd.to_datetime(phase2_data.demo_approx_dob_new)

        # phase 1 age for participants whose age was incorrect in phase1
        phase1_data['new_phase1age'] = abs(phase1_data['enrolment_date'] - phase2_data['demo_dob_new'])/365
        phase1_data['new_phase1age'] = phase1_data['new_phase1age'] / np.timedelta64(1, 'D')
        phase1_data['new_phase1age'] = phase1_data['new_phase1age'].round(0)

        # phase 1 approximate age for participants whose approximate age was incorrect in phase1
        phase1_data['new_phaseaprox1age'] = abs(phase1_data['enrolment_date'] - phase2_data['demo_approx_dob_new']) / 365
        phase1_data['new_phaseaprox1age'] = phase1_data['new_phaseaprox1age'] / np.timedelta64(1, 'D')
        phase1_data['new_phaseaprox1age'] = phase1_data['new_phaseaprox1age'].round(0)

        # phase 1 approximate age for participants whose approximate age was incorrect in phase1
        phase1_data['diff_enrolmentdates'] = abs(phase2_data['gene_enrolment_date'] - phase1_data['enrolment_date']) / 365
        phase1_data['diff_enrolmentdates'] = phase1_data['diff_enrolmentdates'] / np.timedelta64(1, 'D')
        phase1_data['diff_enrolmentdates'] = phase1_data['diff_enrolmentdates'].round(0)

        # qc age
        phase1_data['age_c'] = np.where((phase2_data['demo_dob_new'].notnull()), phase1_data.new_phase1age, phase1_data.age)
        phase1_data['age_c'] = np.where((phase2_data['demo_approx_dob_new'].notnull()), phase1_data.new_phaseaprox1age, phase1_data.age_c)

        return phase1_data