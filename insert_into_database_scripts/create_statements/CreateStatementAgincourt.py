def CreateStatementAgincourt():
    create_script = '''CREATE TABLE IF NOT EXISTS agincourt_redcap_data(
            study_id text PRIMARY KEY,
            redcap_event_name text,
            gene_site_id integer, 
            --gene_uni_site_id_is_correct integer, not in agincourt
            gene_uni_site_id_correct text,
            gene_site integer,
            gene_enrolment_date DATE, 
            gene_start_time text, -- to change to time after being able to encoperate NULL values
            gene_end_time text,
            gene_compensation integer,
            demo_approx_dob_is_correct integer, -- to include a NULL constraint
            demo_dob_is_correct integer,
            demo_date_of_birth_known integer,
            --demo_dob_new text, --to change to DATE POPI
            --demo_approx_dob_new text, --to change to DATE POPI
            demo_age_at_collection integer,
            demo_gender_is_correct integer,
            demo_gender_correction integer,
            demo_gender integer,
            home_language_confirmation integer,
            home_language integer,
            other_home_language text,
            demo_home_language integer,
            ethnicity_confirmation integer,
            ethnicity integer,
            other_ethnicity text,
            gene_identity_confirmed integer,
            participant_identification_complete integer,
            ethn_father_ethn_sa integer,
            ethn_father_lang_sa integer,
            ethn_pat_gfather_ethn_sa integer,
            ethn_pat_gfather_lang_sa integer,
            ethn_pat_gmother_ethn_sa integer,
            ethn_pat_gmother_lang_sa integer,
            ethn_mother_ethn_sa integer,
            ethn_mother_lang_sa integer,
            ethn_mat_gfather_ethn_sa integer,
            ethn_mat_gfather_lang_sa integer,
            ethn_mat_gmother_ethn_sa integer,
            ethn_mat_gmother_lang_sa integer,
            ethnolinguistic_information_complete integer,
            famc_siblings integer,
            famc_number_of_brothers integer,
            famc_living_brothers integer,
            famc_number_of_sisters integer,
            famc_living_sisters integer,
            famc_children integer,
            famc_bio_sons integer,
            famc_living_bio_sons integer,
            famc_bio_daughters integer,
            famc_living_bio_daughters integer,
            family_composition_complete integer,
            preg_pregnant integer,
            preg_num_of_pregnancies integer,
            preg_num_of_live_births integer,
            preg_birth_control integer,
            preg_hysterectomy integer,
            preg_regular_periods integer,
            preg_last_period_remember integer,
            preg_last_period_mon integer,
            preg_last_period_mon_2 integer,
            preg_period_more_than_yr integer,
            pregnancy_and_menopause_complete integer,
            mari_marital_status integer,
            educ_highest_level integer,
            educ_highest_years integer,
            educ_formal_years integer,
            empl_status integer,
            empl_days_work integer,
            civil_status_marital_status_education_employment_complete integer,
            cogn_read_sentence integer,
            cogn_memory integer,
            cogn_difficulty_remember integer,
            cogn_difficulty_concern integer,
            cogn_learning_new_task integer,
            cogn_words_remember_p1___1 integer,
            cogn_words_remember_p1___2 integer, 
            cogn_words_remember_p1___3 integer,
            cogn_words_remember_p1___4 integer,
            cogn_words_remember_p1___5 integer,
            cogn_words_remember_p1___6 integer,
            cogn_words_remember_p1___7 integer,
            cogn_words_remember_p1___8 integer, 
            cogn_words_remember_p1___9 integer,
            cogn_words_remember_p1___10 integer,
            cogn_words_remember_p1____8 integer,
            cogn_words_remember_p1____999 integer, 
            cogn_imm_recall_score_p1 integer,
            cogn_year integer,
            cogn_what_is_the_month integer,
            cogn_day_of_the_month integer,
            cogn_country_of_residence integer,
            cogn_district_province integer,
            cogn_village_town_city integer,
            cogn_weekdays_forward integer,
            cogn_weekdays_backwards integer,
            cogn_orientation_score integer,
            a_cognition_one_complete integer,
            frai_standing_up_time numeric,
            frai_use_hands integer,
            frai_sit_stands_completed integer,
            frai_comment text,
            frai_non_dominant_hand integer,
            frai_dynometer_force_1 numeric,
            frai_dynometer_force_2 numeric,
            frai_dynometer_force_3 numeric,
            frai_complete_procedure integer,
            frai_comment_why text,
            frai_turn_walk_back numeric,
            frai_need_support integer,
            frai_procedure_walk_comp integer,
            frai_please_comment_why text,
            b_frailty_measurements_complete integer,
            cogn_delayed_recall___1 integer,
            cogn_delayed_recall___2 integer,
            cogn_delayed_recall___3 integer,
            cogn_delayed_recall___4 integer,
            cogn_delayed_recall___5 integer,
            cogn_delayed_recall___6 integer,
            cogn_delayed_recall___7 integer,
            cogn_delayed_recall___8 integer,
            cogn_delayed_recall___9 integer,
            cogn_delayed_recall___10 integer,
            cogn_delayed_recall____8 integer,
            cogn_delayed_recall____999 integer,
            cogn_delayed_recall_score integer, 
            cogn_word_cognition_list___1 integer,
            cogn_word_cognition_list___2 integer,
            cogn_word_cognition_list___3 integer,
            cogn_word_cognition_list___4 integer,
            cogn_word_cognition_list___5 integer,
            cogn_word_cognition_list___6 integer,
            cogn_word_cognition_list___7 integer,
            cogn_word_cognition_list___8 integer,
            cogn_word_cognition_list___9 integer,
            cogn_word_cognition_list___10 integer,
            cogn_word_cognition_list___11 integer,
            cogn_word_cognition_list___12 integer,
            cogn_word_cognition_list___13 integer,
            cogn_word_cognition_list___14 integer,
            cogn_word_cognition_list___15 integer,
            cogn_word_cognition_list___16 integer,
            cogn_word_cognition_list___17 integer,
            cogn_word_cognition_list___18 integer,
            cogn_word_cognition_list___19 integer,
            cogn_word_cognition_list___20 integer,
            cogn_word_cognition_list____8 integer,
            cogn_word_cognition_list____999 integer,
            cogn_recognition_score integer,
            cogn_different_animals integer,
            cogn_comments text,
            c_cognition_two_complete integer,
            hous_household_size integer,
            hous_number_of_rooms integer,
            hous_number_of_bedrooms integer,
            hous_electricity integer,
            hous_solar_energy integer,
            hous_power_generator integer,
            hous_alter_power_src integer,
            hous_television integer,
            hous_radio integer,
            hous_motor_vehicle integer,
            hous_motorcycle integer,
            hous_bicycle integer,
            hous_refrigerator integer,
            hous_washing_machine integer,
            hous_sewing_machine integer,
            hous_telephone integer,
            hous_mobile_phone integer,
            hous_microwave integer,
            hous_dvd_player integer,
            hous_satellite_tv_or_dstv integer,
            hous_computer_or_laptop integer,
            hous_internet_by_computer integer,
            hous_internet_by_m_phone integer,
            hous_electric_iron integer,
            hous_fan integer,
            hous_electric_gas_stove integer,
            hous_kerosene_stove integer,
            hous_plate_gas integer,
            hous_electric_plate integer,
            hous_torch integer,
            hous_gas_lamp integer,
            hous_kerosene_lamp integer,
            hous_toilet_facilities integer,
            hous_portable_water integer, 
            hous_grinding_mill integer,
            hous_table integer,
            hous_sofa integer,
            hous_wall_clock integer,
            hous_bed integer,
            hous_mattress integer,
            hous_blankets integer,
            hous_cattle integer,
            hous_other_livestock integer,
            hous_poultry integer,
            hous_tractor integer,
            hous_plough integer,
            --ses_c integer GENERATED ALWAYS AS (
            --  CASE
            --    WHEN hous_electricity IS NULL AND
            --         hous_solar_energy IS NULL AND
            --         hous_power_generator IS NULL AND
            --         hous_alter_power_src IS NULL AND
            --         hous_television IS NULL AND
            --         hous_radio IS NULL AND
            --         hous_motor_vehicle IS NULL AND
            --         hous_motorcycle IS NULL AND
            --         hous_bicycle IS NULL AND
            --         hous_refrigerator IS NULL AND
            --         hous_washing_machine IS NULL AND
            --         hous_sewing_machine IS NULL AND
            --         hous_telephone IS NULL AND
            --         hous_mobile_phone IS NULL AND
            --         hous_microwave IS NULL AND
            --         hous_dvd_player IS NULL AND
            --         hous_satellite_tv_or_dstv IS NULL AND
            --         hous_computer_or_laptop IS NULL AND
            --        hous_internet_by_computer IS NULL AND
            --         hous_internet_by_m_phone IS NULL AND
            --         hous_electric_iron IS NULL AND
            --         hous_fan IS NULL AND
            --         hous_electric_gas_stove IS NULL AND
            --         hous_kerosene_stove IS NULL AND
            --         hous_plate_gas IS NULL AND
            --         hous_electric_plate IS NULL AND
            --         hous_torch IS NULL AND
            --         hous_gas_lamp IS NULL AND
            --         hous_kerosene_lamp IS NULL AND
            --         hous_toilet_facilities IS NULL AND
            --         hous_portable_water IS NULL AND
            --         hous_grinding_mill IS NULL AND
            --         hous_gas_lamp IS NULL AND
            --         hous_table IS NULL AND
            --         hous_sofa IS NULL AND
            --         hous_wall_clock IS NULL AND
            --         hous_bed IS NULL AND
            --         hous_mattress IS NULL AND
            --         hous_blankets IS NULL AND
            --         hous_cattle IS NULL AND
            --         hous_other_livestock IS NULL AND
            --         hous_poultry IS NULL AND
            --         hous_tractor IS NULL AND
            --         hous_plough IS NULL
            --         THEN NULL
            --    ELSE
            --         COALESCE(hous_electricity, 0) +
            --         COALESCE(hous_solar_energy, 0) +
            --         COALESCE(hous_power_generator, 0) +
            --         COALESCE(hous_alter_power_src, 0) +
            --         COALESCE(hous_television, 0) +
            --         COALESCE(hous_radio, 0) +
            --         COALESCE(hous_motor_vehicle, 0) +
            --         COALESCE(hous_motorcycle, 0) +
            --         COALESCE(hous_bicycle, 0) +
            --         COALESCE(hous_refrigerator, 0) +
            --         COALESCE(hous_washing_machine, 0) +
            --         COALESCE(hous_sewing_machine, 0) +
            --         COALESCE(hous_telephone, 0) +
            --         COALESCE(hous_mobile_phone, 0) +
            --         COALESCE(hous_microwave, 0) +
            --         COALESCE(hous_dvd_player, 0) +
            --        COALESCE(hous_satellite_tv_or_dstv, 0) +
            --         COALESCE(hous_computer_or_laptop, 0) +
            --         COALESCE(hous_internet_by_computer, 0) +
            --         COALESCE(hous_internet_by_m_phone, 0) +
            --         COALESCE(hous_electric_iron, 0) +
            --         COALESCE(hous_fan, 0) +
            --         COALESCE(hous_electric_gas_stove, 0) +
            --         COALESCE(hous_kerosene_stove, 0) +
            --         COALESCE(hous_plate_gas, 0) +
            --         COALESCE(hous_electric_plate, 0) +
            --         COALESCE(hous_torch, 0) +
            --         COALESCE(hous_gas_lamp, 0) +
            --         COALESCE(hous_kerosene_lamp, 0) +
            --         COALESCE(hous_toilet_facilities, 0) +
            --         COALESCE(hous_portable_water, 0) +
            --         COALESCE(hous_grinding_mill, 0) +
            --         COALESCE(hous_gas_lamp, 0) +
            --         COALESCE(hous_table, 0) +
            --         COALESCE(hous_sofa, 0) +
            --         COALESCE(hous_wall_clock, 0) +
            --         COALESCE(hous_bed, 0) +
            --         COALESCE(hous_mattress, 0) +
            --         COALESCE(hous_blankets, 0) +
            --         COALESCE(hous_cattle, 0) +
            --         COALESCE(hous_other_livestock, 0) +
            --         COALESCE(hous_poultry, 0) +
            --         COALESCE(hous_tractor, 0) +
            --         COALESCE(hous_plough, 0)
            --  END
            --) STORED,
            household_attributes_complete integer,
            subs_tobacco_use integer,
            subs_smoke_100 integer,
            subs_smoke_now integer,
            subs_smoke_last_hour integer,
            subs_smoke_cigarettes___1 integer,
            subs_smoke_cigarettes___2 integer,
            subs_smoke_cigarettes___3 integer,
            subs_smoke_cigarettes___4 integer,
            subs_smoke_cigarettes___5 integer,
            subs_smoke_cigarettes____8 integer,
            subs_smoke_cigarettes____999 integer,
            subs_smoke_specify text,
            subs_smoking_frequency integer,
            subs_smoke_per_day integer,
            subs_smoking_start_age text, 
            subs_smoking_stop_year integer,
            subs_smokeless_tobacc_use integer,
            subs_snuff_use integer,
            subs_snuff_method_use integer,
            subs_snuff_use_freq integer,
            subs_freq_snuff_use integer,
            subs_tobacco_chew_use integer, 
            subs_tobacco_chew_freq integer,
            subs_tobacco_chew_d_freq integer,
            subs_alcohol_consump integer,
            subs_alcohol_consume_now integer,
            subs_alcohol_consump_freq integer,
            subs_alcohol_consume_freq integer,
            subs_alcohol_cutdown integer,
            subs_alcohol_criticize integer,
            subs_alcohol_guilty integer,
            subs_alcohol_hangover integer, 
            subs_alcohol_con_past_yr integer,
            subs_alcoholtype_consumed___1 integer,
            subs_alcoholtype_consumed___2 integer,
            subs_alcoholtype_consumed___3 integer,
            subs_alcoholtype_consumed___4 integer,
            subs_alcoholtype_consumed___5 integer,
            subs_alcoholtype_consumed____999 integer,
            subs_alcohol_specify text,
            subs_drugs_use integer,
            subs_drug_use_other integer,
            substance_use_complete integer, 
            genh_breast_cancer integer,
            genh_breast_cancer_treat integer,
            genh_bre_cancer_treat_now integer,
            genh_breast_cancer_meds text,
            genh_bre_cancer_trad_med integer,
            genh_cervical_cancer integer,
            genh_cer_cancer_treat integer,
            genh_cer_cancer_treat_now integer,
            genh_cervical_cancer_meds text,
            genh_cer_cancer_trad_med integer,
            genh_prostate_cancer integer,
            genh_pro_cancer_treat integer,
            genh_pro_cancer_treat_now integer,
            genh_prostate_cancer_meds text,
            genh_pro_cancer_trad_med integer,
            genh_oesophageal_cancer integer,
            genh_oes_cancer_treat integer,
            genh_oes_cancer_treat_now integer,
            genh_oes_cancer_meds text,
            genh_oesophageal_trad_med integer,
            genh_other_cancers integer,
            genh_cancer_specify_other text,
            genh_other_cancer_treat integer,
            genh_oth_cancer_treat_now integer,
            genh_other_cancer_meds text,
            genh_oth_cancer_trad_med integer,
            a_general_health_cancer_complete integer,
            genh_obesity_mom integer,
            genh_h_blood_pressure_mom integer,
            genh_h_cholesterol_mom integer,
            genh_breast_cancer_mom integer,
            genh_cervical_cancer_mom integer,
            genh_oes_cancer_mom integer,
            genh_cancer_other_mom integer,
            genh_asthma_mom integer,
            genh_obesity_dad integer,
            genh_h_blood_pressure_dad integer,
            genh_h_cholesterol_dad integer,
            genh_prostate_cancer_dad integer,
            genh_other_cancers_dad integer,
            genh_asthma_dad integer,
            b_general_health_family_history_complete integer,
            genh_days_fruit integer,
            genh_fruit_servings integer,
            genh_days_veg integer,
            genh_veg_servings integer,
            genh_starchy_staple_food___1 integer,
            genh_starchy_staple_food___2 integer, 
            genh_starchy_staple_food___3 integer,
            genh_starchy_staple_food___4 integer, 
            genh_starchy_staple_food___5 integer,
            genh_starchy_staple_food___6 integer,
            genh_starchy_staple_food___7 integer,
            genh_starchy_staple_food___8 integer,
            genh_starchy_staple_food___9 integer,
            genh_starchy_staple_food___10 integer,
            genh_starchy_staple_food___11 integer,
            genh_starchy_staple_food___12 integer,
            genh_starchy_staple_food____8 integer, 
            genh_starchy_staple_food____999 integer,
            genh_starchy_staple_freq integer,
            genh_staple_servings integer,
            genh_vendor_meals integer,
            genh_sugar_drinks integer,
            genh_juice integer,
            genh_change_diet integer,
            genh_lose_weight integer,
            c_general_health_diet_complete integer,
            genh_pesticide integer, 
            genh_pesticide_years integer,
            genh_pesticide_region integer,
            genh_pesticide_type integer,
            genh_pesticide_list text,
            genh_cooking_place integer,
            genh_cookingplace_specify text,
            genh_cooking_done_inside integer,
            genh_energy_source_type___1 integer,
            genh_energy_source_type___2 integer, 
            genh_energy_source_type___3 integer,
            genh_energy_source_type___4 integer,
            genh_energy_source_type___5 integer, 
            genh_energy_source_type___6 integer,
            genh_energy_source_type____999 integer, 
            genh_energy_specify text,
            genh_smoker_in_your_house integer,
            genh_smoke_freq_someone integer,
            genh_insect_repellent_use integer,
            d_general_health_exposure_to_pesticides_pollutants_complete integer,
            infh_malaria integer,
            infh_malaria_month integer,
            infh_malaria_area integer,
            infh_tb integer,
            infh_tb_12months integer,
            infh_tb_diagnosed text,
            infh_tb_treatment integer,
            infh_tb_meds integer,
            infh_tb_counselling integer,
            infh_tb_traditional_med integer,
            infh_hiv_que_answering integer,
            infh_hiv_tested integer,
            infh_hiv_status integer,
            infh_hiv_positive integer,
            infh_hiv_diagnosed text,
            infh_hiv_medication integer,
            infh_hiv_treatment integer,
            infh_hiv_arv_meds text,
            infh_hiv_arv_meds_now integer,
            infh_hiv_arv_meds_specify text,
            infh_hiv_arv_single_pill integer,
            infh_hiv_pill_size integer,
            infh_hiv_traditional_meds integer,
            infh_painful_feet_hands integer,
            infh_hypersensitivity integer,
            infh_kidney_problems integer,
            infh_liver_problems integer,
            infh_change_in_body_shape integer,
            infh_mental_state_change integer,
            infh_chol_levels_change integer,
            infh_hiv_test integer,
            infh_hiv_counselling integer,
            infection_history_complete integer,
            carf_blood_sugar integer,
            carf_diabetes integer,
            carf_diabetes_12months integer,
            carf_diabetes_treatment integer,
            carf_diabetes_treat_now integer,
            carf_diabetes_treat___1 integer,
            carf_diabetes_treat___2 integer,
            carf_diabetes_treat___3 integer,
            carf_diabetes_treat___4 integer,
            carf_diabetes_treat___5 integer, 
            carf_diabetes_treat____999 integer,
            carf_diabetetreat_specify text,
            carf_diabetes_meds_2 text,
            carf_diabetes_traditional integer,
            carf_diabetes_history integer,
            carf_diabetes_mother integer,
            carf_diabetes_father integer,
            carf_diabetes_brother_1 integer,
            carf_diabetes_brother_2 integer,
            carf_diabetes_brother_3 integer,
            carf_diabetes_brother_4 integer,
            carf_diabetes_sister_1 integer,
            carf_diabetes_sister_2 integer,
            carf_diabetes_sister_3 integer, 
            carf_diabetes_sister_4 integer,
            carf_diabetes_son_1 integer,
            carf_diabetes_son_2 integer,
            carf_diabetes_son_3 integer,
            carf_diabetes_son_4 integer,
            carf_daughter_diabetes_1 integer,
            carf_diabetes_daughter_2 integer,
            carf_diabetes_daughter_3 integer,
            carf_diabetes_daughter_4 integer,
            carf_diabetes_fam_other integer,
            carf_diabetes_fam_specify text,
            a_cardiometabolic_risk_factors_diabetes_complete integer,
            carf_stroke integer,
            carf_stroke_diagnosed integer,
            carf_tia integer,
            carf_weakness integer,
            carf_numbness integer,
            carf_blindness integer,
            carf_half_vision_loss integer,
            carf_understanding_loss integer,
            carf_expression_loss integer,
            carf_angina integer,
            carf_angina_treatment integer,
            carf_angina_treat_now integer,
            carf_angina_meds text,
            carf_angina_traditional integer,
            carf_pain integer, 
            carf_pain2 integer,
            carf_pain_action_stopslow integer,
            carf_relief_standstill integer,
            carf_pain_location___1 integer,
            carf_pain_location___2 integer,
            carf_pain_location___3 integer,
            carf_pain_location___4 integer,
            carf_pain_location___5 integer,
            carf_pain_location___6 integer,
            carf_pain_location___7 integer,
            carf_pain_location___8 integer,
            carf_pain_location___9 integer,
            carf_pain_location___10 integer, 
            carf_pain_location___11 integer,
            carf_pain_location___12 integer,
            carf_pain_location___13 integer,
            carf_pain_location___14 integer,
            carf_pain_location___15 integer,
            carf_pain_location___16 integer,
            carf_pain_location___17 integer,
            carf_pain_location___18 integer,
            carf_pain_location____999 integer,
            carf_heartattack integer, 
            carf_heartattack_treat integer, 
            carf_heartattack_meds text,
            carf_heartattack_trad integer,
            carf_congestiv_heart_fail integer,
            carf_chf_treatment integer,
            carf_chf_treatment_now integer,
            carf_chf_meds text,
            carf_chf_traditional integer,
            b_cardiometabolic_risk_factors_heart_conditions_complete integer,
            carf_bp_measured integer,
            carf_hypertension integer,
            carf_hypertension_12mnths integer,
            carf_hypertension_treat integer, 
            carf_hypertension_meds integer,
            carf_hypertension_medlist text,
            carf_hypertension_trad integer,
            --carf_htn_present_c integer GENERATED ALWAYS AS (
            --  CASE
            --    WHEN carf_hypertension = 1 THEN 1
            --    WHEN carf_hypertension_meds IS NOT NULL THEN 1
            --    WHEN carf_hypertension_treat = 1 THEN 1
            --    WHEN carf_hypertension_trad = 1 THEN 1
            --  END
            --) STORED,
            --carf_htn_jnc7 integer GENERATED ALWAYS AS (
            -- CASE
            --    WHEN (bppm_systolic_avg >= 140) OR (bppm_diastolic_avg >= 90) THEN 1
            --    WHEN carf_hypertension = 1 THEN 1
            --    WHEN (carf_hypertension = 0 OR carf_hypertension = 2) AND (bppm_systolic_avg < 140) AND (bppm_diastolic_avg < 90) THEN 0
            --  END
            --) STORED,
            carf_cholesterol integer,
            carf_h_cholesterol integer,
            carf_chol_treatment integer, 
            carf_chol_treatment_now___1 integer,
            carf_chol_treatment_now___2 integer, 
            carf_chol_treatment_now___3 integer,
            carf_chol_treatment_now___4 integer,
            carf_chol_treatment_now____999 integer,
            carf_chol_treat_specify text,
            carf_chol_medicine text,
            carf_chol_traditional integer,
            c_cardiometabolic_risk_factors_hypertension_choles_complete integer,
            carf_thyroid integer,
            carf_thyroid_type integer,
            carf_thryroid_specify text,
            carf_thyroid_treatment integer,
            carf_thyroid_treat_use integer,
            carf_parents_thyroid integer,
            carf_thyroidparnt_specify integer,
            carf_kidney_disease integer,
            carf_kidney_disease_known integer,
            carf_kidneydiseas_specify text,
            carf_kidney_function_low integer,
            carf_kidney_family integer,
            carf_kidney_family_mother integer,
            carf_kidney_family_father integer,
            carf_kidney_family_other integer,
            carf_kidney_fam_specify text,
            carf_kidney_family_type integer,
            carf_kidney_fam_tspecify text,
            carf_joints_swollen_pain integer,
            carf_joints_swollen integer,
            carf_joints_involved integer,
            carf_when_they_hurt integer,
            carf_symptoms_how_long integer,
            carf_arthritis_results integer,
            carf_rheumatoid_factor integer,
            carf_acpa integer,
            carf_esr_crp integer,
            carf_osteo integer,
            carf_osteo_sites___1 integer,
            carf_osteo_sites___2 integer,
            carf_osteo_sites___3 integer,
            carf_osteo_sites___4 integer,
            carf_osteo_sites___5 integer,
            carf_osteo_sites___6 integer,
            carf_osteo_sites____999 integer,
            carf_osteo_hip_replace integer,
            carf_osteo_hip_repl_site integer,
            carf_osteo_hip_repl_age integer,
            carf_osteo_knee_replace integer,
            carf_osteo_knee_repl_site integer,
            carf_osteo_knee_repl_age integer,
            d_cardiometabolic_risk_factors_kidney_thyroid_ra_complete integer,
            gpaq_work_weekend integer,
            gpaq_work_sedentary integer,
            gpaq_work_vigorous integer,
            gpaq_work_vigorous_days integer,
            gpaq_work_vigorous_time integer, 
            gpaq_work_vigorous_hrs integer,
            gpaq_work_vigorous_mins integer,
            gpaq_work_moderate integer,
            gpaq_work_moderate_days integer,
            gpaq_work_moderate_time integer, 
            gpaq_work_moderate_hrs integer,
            gpaq_work_moderate_mins integer,
            gpaq_work_day_time integer, 
            gpaq_work_day_hrs integer, 
            gpaq_work_day_mins integer,
            gpaq_transport_phy integer, 
            gpaq_transport_phy_days integer,
            gpaq_transport_phy_time integer, 
            gpaq_transport_phy_hrs integer,
            gpaq_transport_phy_mins integer,
            gpaq_leisure_phy integer,
            gpaq_leisure_vigorous integer,
            gpaq_leisurevigorous_days integer,
            gpaq_leisurevigorous_time integer, 
            gpaq_leisurevigorous_hrs integer,
            gpaq_leisurevigorous_mins integer,
            gpaq_leisuremoderate integer,
            gpaq_leisuremoderate_days integer,
            gpaq_leisuremoderate_time integer, 
            gpaq_leisuremoderate_hrs integer,
            gpaq_leisuremoderate_mins integer,
            --gpaq_exercise_duration_c integer GENERATED ALWAYS AS (
            --  CASE
            --    WHEN (gpaq_work_vigorous_time < 30 AND
            --         gpaq_work_moderate_time < 30 AND
            --         gpaq_transport_phy_time < 30 AND
            --         gpaq_leisurevigorous_time < 30 AND
            --         gpaq_leisuremoderate_time < 30) THEN 0
            --    WHEN ((gpaq_work_vigorous_time >= 30 AND gpaq_work_vigorous_time <= 60) OR
            --         (gpaq_work_moderate_time >= 30 AND gpaq_work_moderate_time <= 60) OR
            --         (gpaq_transport_phy_time >= 30 AND gpaq_transport_phy_time <= 60) OR
            --         (gpaq_leisurevigorous_time >= 30 AND gpaq_leisurevigorous_time <= 60) OR
            --         (gpaq_leisuremoderate_time >= 30 AND gpaq_leisuremoderate_time <= 60)) THEN 1
            --    WHEN (gpaq_work_vigorous_time > 60 OR
            --         gpaq_work_moderate_time > 60 OR
            --         gpaq_transport_phy_time > 60 OR
            --         gpaq_leisurevigorous_time > 60 OR
            --         gpaq_leisuremoderate_time > 60) THEN 2
            --  END
            --) STORED,
            --gpaq_exercise_frequency_c integer GENERATED ALWAYS AS (
            --  CASE
            --    WHEN (gpaq_work_vigorous_days = 0 AND
            --         gpaq_work_moderate_days = 0 AND
            --         gpaq_transport_phy_days = 0 AND
            --         gpaq_leisurevigorous_days = 0 AND
            --         gpaq_leisuremoderate_days = 0) THEN 0
            --    WHEN ((gpaq_work_vigorous_days >= 0 AND gpaq_work_vigorous_days <= 3) AND
            --         (gpaq_work_moderate_days >= 0 AND gpaq_work_moderate_days <= 3) AND
            --         (gpaq_transport_phy_days >= 0 AND gpaq_transport_phy_days <= 3) AND
            --         (gpaq_leisurevigorous_days >= 0 AND gpaq_leisurevigorous_days <= 3) AND
            --         (gpaq_leisuremoderate_days >= 0 AND gpaq_leisuremoderate_days <= 3)) THEN 1
            --    WHEN (gpaq_work_vigorous_days > 3 AND
            --         gpaq_work_moderate_days > 3 AND
            --         gpaq_transport_phy_days > 3 AND
            --         gpaq_leisurevigorous_days > 3 AND
            --         gpaq_leisuremoderate_days > 3) THEN 2
            --  END
            --) STORED,
            --gpaq_exercise_status_c integer GENERATED ALWAYS AS (
            --  CASE
            --    WHEN (gpaq_work_vigorous = 0 AND
            --         gpaq_work_moderate = 0 AND
            --         gpaq_transport_phy = 0 AND
            --         gpaq_leisure_phy = 0 AND
            --         gpaq_leisure_vigorous = 0 AND
            --         gpaq_leisuremoderate = 0) THEN 0
            --    WHEN ((gpaq_work_vigorous = 0 OR gpaq_work_vigorous = 1) AND
            --         (gpaq_work_moderate = 0 OR gpaq_work_moderate = 1) AND
            --         (gpaq_transport_phy = 0 OR gpaq_transport_phy = 1) AND
            --         (gpaq_leisure_phy = 0 OR gpaq_leisure_phy = 1) AND
            --         (gpaq_leisure_vigorous = 0 OR gpaq_leisure_vigorous = 1) AND
            --         (gpaq_leisuremoderate = 0 OR gpaq_leisuremoderate = 1)) THEN 1
            --    WHEN (gpaq_work_vigorous = 1 AND
            --         gpaq_work_moderate = 1 AND
            --         gpaq_transport_phy = 1 AND
            --         gpaq_leisure_phy = 1 AND
            --         gpaq_leisure_vigorous = 1 AND
            --         gpaq_leisuremoderate = 1) THEN 2
            --  END
            --) STORED,
            gpaq_work_day_stng_time integer,  
            gpaq_work_day_stng_hrs integer,
            gpaq_work_day_stng_mins integer,
            gpaq_non_work_day_time integer, 
            gpaq_non_work_day_hrs integer,
            gpaq_non_work_day_mins integer,
            gpaq_week_sleep_time text,
            gpaq_week_wakeup_time text,
            gpaq_weekend_sleep_time text,
            gpaq_weekend_wakeup_time text,
            gpaq_sleep_room_pple_num integer, 
            gpaq_sleep_room_livestock integer,
            gpaq_sleep_on integer,
            gpaq_mosquito_net_use integer,
            gpaq_feel_alert integer, 
            gpaq_sleeping_difficulty integer,
            gpaq_difficulty_staysleep integer,
            gpaq_waking_early_problem integer,
            gpaq_waking_up_tired integer, 
            gpaq_sleep_pattern_satis integer,
            gpaq_sleep_interfere integer,
            physical_activity_and_sleep_complete integer,
            anth_standing_height numeric,
            anth_weight numeric,
            --anth_bmi_c numeric GENERATED ALWAYS AS (
            --  CASE
            --    WHEN anth_weight < 0 THEN -999
            --    WHEN anth_standing_height < 0 THEN -999
            --    ELSE ( anth_weight / (anth_standing_height/1000) / (anth_standing_height/1000) )
            --  END
            --) STORED,
            anth_waist_circumf_1 numeric, 
            anth_waist_circumf_2 numeric, 
            anth_waist_circumf numeric, 
            anth_hip_circumf_1 numeric,
            anth_hip_circumf_2 numeric,
            anth_hip_circumf numeric,
            anth_measurementcollector text,
            anthropometric_measurements_complete integer, 
            bppm_systolic_1 integer,
            bppm_diastolic_1 integer,
            bppm_pulse_1 integer,
            bppm_measurement_time_1 text,
            bppm_systolic_2 integer,
            bppm_diastolic_2 integer,
            bppm_pulse_2 integer,
            bppm_measurement_time_2 text,
            bppm_systolic_3 integer, 
            bppm_diastolic_3 integer,
            bppm_pulse_3 integer,
            bppm_measurement_time_3 text,
            bppm_measurementcollector text,
            bppm_systolic_avg numeric,
            bppm_diastolic_avg numeric, 
            bppm_pulse_avg numeric,
            blood_pressure_and_pulse_measurements_complete integer,
            ultr_vat_scat_measured integer,
            ultr_comment text,
            ultr_technician text,
            ultr_visceral_fat numeric,
            ultr_subcutaneous_fat numeric,
            ultr_cimt integer,
            ultr_cimt_comment text,
            ultr_cimt_technician text,
            ultr_cimt_right_min numeric,
            ultr_cimt_right_max numeric,
            ultr_cimt_right_mean numeric,
            ultr_cimt_left_min numeric,
            ultr_cimt_left_max numeric,
            ultr_cimt_left_mean numeric,
            ultr_plaque_measured integer,
            ultr_plaque_comment text,
            ultr_plaque_technician text,
            ultr_plaque_right_present integer,
            ultr_plaque_right numeric,
            ultr_plaque_left_present integer,
            ultr_plaque_left numeric,
            ultr_dxa_scan_completed integer,
            ultr_dxa_scan_comment text,
            ultr_dxa_measurement_1 numeric,
            ultr_dxa_measurement_2 numeric,
            ultr_dxa_measurement_3 numeric,
            ultr_dxa_measurement_4 numeric,
            ultr_dxa_measurement_5 numeric,
            ultrasound_and_dxa_measurements_complete integer,
            resp_breath_shortness integer,
            resp_breath_shortness_ever integer,
            resp_mucus integer,
            resp_breath_too_short integer,
            resp_cough integer,
            resp_wheezing_whistling integer,
            resp_asthma_diagnosed integer,
            resp_age_diagnosed integer,
            resp_asthma_treat integer,
            resp_asthma_treat_now integer,
            resp_copd_suffer___1 integer,
            resp_copd_suffer___2 integer,
            resp_copd_suffer___3 integer,
            resp_copd_suffer___0 integer,
            resp_copd_suffer___9 integer,
            resp_copd_suffer____999 integer,
            resp_copd_treat integer,
            resp_inhaled_medication integer,
            resp_medication_list text,
            resp_puffs_time integer,
            resp_puffs_times_day integer,
            resp_measles_suffer___1 integer,
            resp_measles_suffer___2 integer,
            resp_measles_suffer___0 integer,
            resp_measles_suffer___9 integer,
            resp_measles_suffer____999 integer,
            a_respiratory_health_complete integer,
            rspe_major_surgery integer,
            rspe_chest_pain integer, 
            rspe_coughing_blood integer,
            rspe_acute_retinal_detach integer,
            rspe_any_pain integer, 
            rspe_diarrhea integer, 
            rspe_high_blood_pressure integer,
            rspe_tb_diagnosed integer,
            rspe_tb_treat_past4wks integer,
            rspe_infection___1 integer, 
            rspe_infection___2 integer,
            rspe_infection___3 integer,
            rspe_infection___4 integer,
            rspe_infection___0 integer,
            rspe_infection____999 integer,
            rspe_participation integer,
            rspe_wearing_tightclothes integer,
            rspe_wearing_dentures integer,
            rspe_participation_note integer, 
            rspe_researcher_question integer,
            b_spirometry_eligibility_complete integer, 
            spiro_eligible integer,
            spiro_researcher text,
            spiro_num_of_blows integer,
            spiro_num_of_vblows integer,
            spiro_pass integer,
            spiro_comment text,
            c_spirometry_test_complete integer,
            rspir_salb_admin integer,
            rspir_salb_time_admin text,
            rspir_time_started text,
            rspir_researcher text,
            rspir_num_of_blows integer,
            rspir_num_of_vblows integer,
            rspir_comment text,
            d_reversibility_test_complete integer,
            micr_take_antibiotics integer,
            micr_diarrhea_last_time integer,
            micr_worm_intestine_treat integer, 
            micr_probiotics_t_period integer, 
            micr_wormintestine_period integer,
            micr_probiotics_taken integer,
            a_microbiome_complete integer,
            bloc_last_eat_time text, -- to change to time
            bloc_last_ate_hrs numeric, 
            bloc_last_drink_time text,
            bloc_hours_last_drink numeric, 
            bloc_fasting_confirmed integer, 
            bloc_two_red_tubes integer,
            bloc_red_tubes_num integer,
            bloc_one_purple_tube integer,
            bloc_if_no_purple_tubes integer,
            bloc_one_grey_tube integer, 
            bloc_grey_tubes_no integer,
            bloc_phlebotomist_name text,
            bloc_blood_taken_date text, --to change to date
            bloc_bloodcollection_time text,
            b_blood_collection_complete integer,
            bloc_urine_collected integer,
            bloc_specify_reason text,
            bloc_urcontainer_batchnum text,
            bloc_urine_tube_expiry text,
            bloc_urine_collector text,
            bloc_urine_taken_date text, --to change to date
            bloc_urinecollection_time text, --to change to time
            c_urine_collection_complete integer, 
            poc_test_conducted integer,
            poc_comment text,
            poc_instrument_serial_num text,
            poc_test_strip_batch_num text,
            poc_teststrip_expiry_date text, --its mixed, dates and options of dates... confusion
            poc_test_date text, -- to change to DATE
            poc_test_time text,
            poc_researcher_name text,
            poc_glucose_test_result numeric,
            poc_chol_result numeric,
            poc_gluc_results_provided integer,
            poc_gluc_results_notes text,
            poc_chol_results_provided integer,
            poc_chol_results_notes text,
            poc_glucresults_discussed integer,
            poc_cholresults_discussed integer,
            poc_seek_advice integer,
            poc_hiv_test_conducted integer,
            poc_hiv_comment text,
            poc_hiv_pre_test integer,
            poc_pre_test_worker text,
            poc_test_kit_serial_num text,
            poc_hiv_strip_batch_num text,
            poc_hiv_strip_expiry_date text,--its options of date
            poc_hiv_test_date_done text, --to change toDATE
            poc_technician_name text,
            poc_hiv_test_result integer, 
            poc_result_provided integer,
            poc_post_test_counselling integer, 
            poc_post_test_worker integer,
            poc_hivpositive_firsttime integer,
            poc_hiv_seek_advice integer,
            point_of_care_testing_complete integer, 
            tram_injury_ill_assault integer,
            tram_relative_ill_injured integer, 
            tram_deceased integer,
            tram_family_friend_died integer, 
            tram_marital_separation integer,
            tram_broke_relationship integer,
            tram_problem_with_friend integer,
            tram_unemployed integer,
            tram_sacked_from_your_job integer,
            tram_financial_crisis integer,
            tram_problems_with_police integer,
            tram_some_valued_lost integer,
            trauma_complete integer,
            comp_sections_1_13 integer,
            comp_comment_no_1_13 text,
            comp_section_14 integer,
            comp_comment_no_14 text,
            comp_section_15 integer,
            comp_comment_no_15 text,
            comp_section_16 integer,
            comp_comment_no_16 text,
            comp_section_17 integer,
            comp_comment_no_17 text,
            comp_section_18 integer,
            comp_comment_no_18 text,
            comp_section_19 integer,
            comp_comment_no_19 text,
            comp_section_20 integer,
            comp_comment_no_20 text,
            --comp_end_time text,
            completion_of_questionnaire_complete integer
            --gene_end_time text
            --subs_smoke_cigarettes___6 integer, not in soweto
            --genh_oes_cancer_dad integer,
            --genh_starchy_staple_food___13 integer,
            --genh_starchy_staple_food___14 integer,
            --genh_starchy_staple_food___15 integer,
            --genh_starchy_staple_food___16 integer,
            --carf_diabetes_treat___6 integer,
            --bloc_two_purple_tube integer

        ) 
            '''
    return create_script