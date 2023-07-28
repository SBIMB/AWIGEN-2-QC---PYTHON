def CreateStatementAllPhase2():
    create_script = '''CREATE TABLE IF NOT EXISTS all_sites_phase2(
            study_id text PRIMARY KEY,
            redcap_event_name text,
            unique_site_id_is_correct integer, --only in soweto
            unique_site_id text,
            site integer,
            enrolment_date DATE, 
            start_time text, -- to change to time after being able to encoperate NULL values
            end_time text,
            compensation_received integer,
            approx_dob_is_correct integer, -- to include a NULL constraint
            dob_is_correct integer,
            date_of_birth_known integer,
            age integer,
            gender_is_correct integer,
            gender_correction integer,
            sex integer,
            sex_encoded text GENERATED ALWAYS AS (
              CASE
                sex
                WHEN 0 THEN 'Female'
                WHEN 1 THEN 'Male'
                ELSE NULL
            END
            ) STORED,
            sex_plink integer GENERATED ALWAYS AS (
              CASE
                sex
                WHEN 0 THEN 2
                WHEN 1 THEN 1
                ELSE NULL
              END
            ) STORED,
            home_language_confirmation integer,
            home_language integer,
            other_home_language text,
            home_language_c integer,
            ethnicity_confirmation integer,
            ethnicity integer,
            other_ethnicity text,
            country text,
            region text,
            site_type_c integer,
            participant_identity_confirmed integer,
            participant_identification_complete integer,
            father_ethnicity text,
            father_ethnicity_other text,
            father_language text,
            father_language_other text, 
            pat_gfather_ethnicity text,
            pat_gfather_ethnicity_other text,
            pat_gfather_language text,
            pat_gfather_language_other text,
            pat_gmother_ethnicity text,
            pat_gmother_ethnicity_other text,
            pat_gmother_language text,
            pat_gmother_language_other text,
            mother_ethnicity text,
            mother_ethnicity_other text,
            mother_language text,
            mother_language_other text,
            mat_gfather_ethnicity text,
            mat_gfather_ethnicity_other text,
            mat_gfather_language text,
            mat_gfather_language_other text,
            mat_gmother_ethnicity text,
            mat_gmother_ethnicity_other text,
            mat_gmother_language text,
            mat_gmother_language_other text,
            ethnolinguistic_information_complete integer,
            siblings integer,
            number_of_brothers integer,
            number_living_brothers integer,
            number_of_sisters integer,
            number_living_sisters integer,
            children integer,
            number_of_sons integer,
            number_living_bio_sons integer,
            number_of_daughters integer,
            number_living_bio_daughters integer,
            number_of_siblings_c integer, 
            number_of_children_c integer,
            family_composition_complete integer,
            pregnant integer,
            number_of_pregnancies integer,
            number_of_live_births integer,
            birth_control integer,
            hysterectomy integer,
            regular_periods integer,
            last_period_remember integer,
            last_period_mon integer,
            last_period_yr integer,
            period_more_than_yr integer,
            last_period_c text,
            months_last_period_c integer,
            menopause_status_c integer,
            pregnancy_and_menopause_complete integer,
            marital_status integer,
            highest_level_of_education integer,
            years_education integer,
            formal_years_education integer,
            empl_status integer,
            work_days integer,
            partnership_status_c integer,
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
            frai_dynometer_force_max numeric,
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
            cogn_recognition_score integer,
            cogn_different_animals integer,
            cogn_comments text,
            c_cognition_two_complete integer,
            household_size integer,
            number_of_rooms integer,
            number_of_bedrooms integer,
            electricity integer,
            solar_energy integer,
            power_generator integer,
            alter_power_src integer,
            television integer,
            radio integer,
            motor_vehicle integer,
            motorcycle integer,
            bicycle integer,
            refrigerator integer,
            washing_machine integer,
            sewing_machine integer,
            telephone integer,
            mobile_phone integer,
            microwave integer,
            dvd_player integer,
            satellite_tv_or_dstv integer,
            computer_or_laptop integer,
            internet_by_computer integer,
            internet_by_mobile_phone integer,
            electric_iron integer,
            fan integer,
            electric_or_gas_stove integer,
            kerosene_stove integer,
            plate_gas integer,
            electric_plate integer,
            torch integer,
            gas_lamp integer,
            kerosene_lamp_with_glass integer,
            toilet_facilities integer,
            portable_water integer, 
            grinding_mill integer,
            table_kitchen integer,
            sofa_set integer,
            wall_clock integer,
            bed integer,
            mattress integer,
            blankets integer,
            cattle integer,
            other_livestock integer,
            poultry integer,
            tractor integer,
            plough integer,
            people_to_rooms_density_c numeric,
            people_to_bedrooms_density_c numeric,
            ses_c integer,
            ses_site_quintile_c text,
            household_attributes_complete integer,
            tobacco_use integer,
            smoke_greater_than_100 integer,
            current_smoker integer,
            smoke_last_hour integer,
            what_smoke_cigarettes integer,
            what_smoke_pipe integer,
            what_smoke_hand_rolled integer,
            what_smoke_cigars integer,
            what_smoke_other integer,
            what_smoke_6 integer,
            what_smoke_decline integer,
            other_smoke_specify text,
            smoking_frequence integer,
            smoke_per_day integer,
            age_start_smoking integer, 
            year_stop_smoking integer,
            smokeless_tobacco_use integer,
            snuff_use integer,
            snuff_method_use integer,
            frequence_of_snuff_use integer,
            frequence_of_snuff_use_per_day integer,
            chewing_tobacco_use integer, 
            frequence_of_chewing_tobacco_use integer,
            frequence_of_chewing_tobacco_use_per_day integer,
            consume_alcohol integer,
            current_alcohol_consumer integer,
            frequence_of_alcohol_consumption integer,
            amount_of_alcohol_consumed_per_day integer,
            consider_alcohol_cutdown integer,
            criticised_for_drinking integer,
            guilty_for_drinking integer,
            had_hangover integer, 
            drinking_past_year integer,
            alcohol_type_consumed_beer integer,
            alcohol_type_consumed_wine integer,
            alcohol_type_consumed_spirits integer,
            alcohol_type_consumed_home_brew integer,
            alcohol_type_consumed_other integer,
            specify_alcohol_type_consumed text,
            drugs_use integer,
            drug_use_other integer,
            substance_use_complete integer, 
            breast_cancer integer,
            breast_cancer_treat_ever integer,
            breast_cancer_treat_current integer,
            breast_cancer_meds_list text,
            breast_cancer_trad_med integer,
            cervical_cancer integer,
            cervical_cancer_treat_ever integer,
            cervical_cancer_treat_current integer,
            cervical_cancer_meds_list text,
            cervical_cancer_trad_med integer,
            prostate_cancer integer,
            prostate_cancer_treat_ever integer,
            prostate_cancer_treat_current integer,
            prostate_cancer_meds_list text,
            prostate_cancer_trad_med integer,
            oesophageal_cancer integer,
            oesophageal_cancer_treat_ever integer,
            oesophageal_cancer_treat_current integer,
            oesophageal_cancer_meds_list text,
            oesophageal_trad_med integer,
            other_cancers integer,
            other_cancer_specify text,
            other_cancer_treat_ever integer,
            other_cancer_treat_current integer,
            other_cancer_meds_list text,
            other_cancer_trad_med integer,
            cancer_status_c integer,
            a_general_health_cancer_complete integer,
            obesity_mom integer,
            h_blood_pressure_mom integer,
            h_cholesterol_mom integer,
            breast_cancer_mom integer,
            cervical_cancer_mom integer,
            oesophageal_cancer_mom integer,
            other_cancer_mom integer,
            asthma_mom integer,
            obesity_dad integer,
            h_blood_pressure_dad integer,
            h_cholesterol_dad integer,
            prostate_cancer_dad integer,
            oesophageal_cancer_dad integer,
            other_cancers_dad integer,
            asthma_dad integer,
            b_general_health_family_history_complete integer,
            days_fruit integer,
            fruit_servings integer,
            days_veg integer,
            veg_servings integer,
            starchy_staple_food___1 integer,
            starchy_staple_food___2 integer, 
            starchy_staple_food___3 integer,
            starchy_staple_food___4 integer, 
            starchy_staple_food___5 integer,
            starchy_staple_food___6 integer,
            starchy_staple_food___7 integer,
            starchy_staple_food___8 integer,
            starchy_staple_food___9 integer,
            starchy_staple_food___10 integer,
            starchy_staple_food___11 integer,
            starchy_staple_food___12 integer,
            starchy_staple_food___13 integer,
            starchy_staple_food___14 integer,
            starchy_staple_food___15 integer,
            starchy_staple_food___16 integer,
            starchy_staple_food____8 integer, 
            starchy_staple_freq integer,
            staple_servings_per_day integer,
            vendor_meals_per_week integer,
            sugar_drinks integer,
            juice integer,
            change_diet integer,
            lose_weight integer,
            c_general_health_diet_complete integer,
            pesticide integer, 
            years_pesticide integer,
            region_pesticide integer,
            pesticide_type integer,
            pesticide_type_list text,
            cooking_place integer,
            cookingplace_specify text,
            if_inside_ventilation integer,
            energy_source_type___1 integer,
            energy_source_type___2 integer, 
            energy_source_type___3 integer,
            energy_source_type___4 integer,
            energy_source_type___5 integer, 
            energy_source_type___6 integer,
            other_energy_source_specify text,
            smoker_in_your_house integer,
            smoker_in_your_house_freq integer,
            insect_repellent_use integer,
            d_general_health_exposure_to_pesticides_pollutants_complete integer,
            malaria integer,
            malaria_month integer,
            malaria_area integer,
            tb integer,
            tb_12months integer,
            tb_diagnosed text,
            tb_treatment integer,
            tb_meds integer,
            tb_counselling integer,
            tb_trad_med integer,
            hiv_que_answering integer,
            tested_hiv integer,
            hiv_status integer,
            hiv_positive integer,
            hiv_diagnosed_when text,
            hiv_medication_ever integer,
            hiv_treatment_when integer,
            hiv_arv_start_with text,
            hiv_arv_meds_now integer,
            hiv_arv_meds_specify text,
            hiv_arv_single_pill integer,
            hiv_pill_size integer,
            hiv_trad_meds integer,
            painful_feet_hands integer,
            hypersensitivity integer,
            kidney_problems integer,
            liver_problems integer,
            change_in_body_shape integer,
            mental_state_change integer,
            chol_levels_change integer,
            consent_hiv_test integer,
            consent_hiv_counselling integer,
            infection_history_complete integer,
            blood_sugar integer,
            diabetes integer,
            diabetes_12months integer,
            diabetes_treatment integer,
            diabetes_treat_curr integer,
            diabetes_treat_insulin integer,
            diabetes_treat_pills integer,
            diabetes_treat_diet integer,
            diabetes_treat_weight_loss integer,
            diabetes_treat_other integer,
            diabetes_treat_6 integer,
            diabetes_treat_other_specify text,
            diabetes_meds_list text,
            diabetes_trad_meds integer,
            diabetes_history integer,
            diabetes_mother integer,
            diabetes_father integer,
            diabetes_1_brother integer,
            diabetes_2_brother integer,
            diabetes_3_brother integer,
            diabetes_4_brother integer,
            diabetes_1_sister integer,
            diabetes_2_sister integer,
            diabetes_3_sister integer, 
            diabetes_4_sister integer,
            son_1_diabetes integer,
            son_2_diabetes integer,
            son_3_diabetes integer,
            son_4_diabetes integer,
            diabetes_1_daughter integer,
            diabetes_2_daughter integer,
            diabetes_3_daughter integer,
            diabetes_4_daughter integer,
            other_fam_diabetes integer,
            other_fam_diabetes_specify text,
            diabetes_status_c integer,
            pre_qc_diabetes_status_c integer,
            a_cardiometabolic_risk_factors_diabetes_complete integer,
            stroke integer,
            stroke_diagnosed integer,
            transient_ischemic_attack integer,
            weakness integer,
            numbness integer,
            blindness integer,
            half_vision_loss integer,
            understanding_loss integer,
            expression_loss integer,
            angina integer,
            angina_treatment_yn integer,
            angina_treat_current integer,
            angina_meds_list text,
            angina_trad_meds integer,
            pain integer, 
            pain2 integer,
            pain_action_stop_or_slow integer,
            relief_standstill integer,
            pain_location___1 integer,
            pain_location___2 integer,
            pain_location___3 integer,
            pain_location___4 integer,
            pain_location___5 integer,
            pain_location___6 integer,
            pain_location___7 integer,
            pain_location___8 integer,
            pain_location___9 integer,
            pain_location___10 integer, 
            pain_location___11 integer,
            pain_location___12 integer,
            pain_location___13 integer,
            pain_location___14 integer,
            pain_location___15 integer,
            pain_location___16 integer,
            pain_location___17 integer,
            pain_location___18 integer,
            heartattack integer, 
            heartattack_treat_ever integer, 
            heartattack_meds_list text,
            heartattack_trad_meds integer,
            congestive_heart_failure integer,
            chf_treatment_yn integer,
            chf_treat_current integer,
            chf_meds_list text,
            chf_trad_meds integer,
            b_cardiometabolic_risk_factors_heart_conditions_complete integer,
            bp_ever_measured integer,
            hypertension integer,
            hypertension_12months_yn integer,
            hypertension_treat_ever integer, 
            hypertension_meds_current integer,
            hypertension_meds_list text,
            hypertension_trad_meds integer,
            cholesterol_ever_measured integer,
            h_cholesterol integer,
            chol_treatment_ever integer, 
            cholesterol_meds_special_diet integer,
            cholesterol_meds_weight_loss integer, 
            cholesterol_meds_medicine integer,
            cholesterol_meds_other integer,
            other_cholesterol_treat_specify text,
            cholesterol_meds_list text,
            chol_trad_treat integer,
            hypertension_status_c integer,
            c_cardiometabolic_risk_factors_hypertension_choles_complete integer,
            thyroid integer,
            thyroid_type integer,
            thryroid_type_specify text,
            thyroid_treatment_yn integer,
            thyroid_treat_type integer,
            parents_thyroid integer,
            specify_thyroid_parent integer,
            kidney_disease integer,
            know_type_kidney_disease integer,
            type_kidney_disease text,
            low_kidney_function integer,
            kidney_family integer,
            family_kidney_mother integer,
            family_kidney_father integer,
            family_kidney_other integer,
            family_kidney_other_specify text,
            kidney_type_other_family integer,
            kidney_type_other_family_specify text,
            joints_swollen_pain integer,
            joints_swollen integer,
            joints_involved integer,
            when_they_hurt integer,
            symptoms_how_long integer,
            arthritis_results integer,
            rheumatoid_factor integer,
            acpa integer,
            esr_crp integer,
            osteo integer,
            osteo_sites___1 integer,
            osteo_sites___2 integer,
            osteo_sites___3 integer,
            osteo_sites___4 integer,
            osteo_sites___5 integer,
            osteo_sites___6 integer,
            osteo_hip_replace integer,
            osteo_hip_repl_site integer,
            osteo_hip_repl_age integer,
            osteo_knee_replace integer,
            osteo_knee_repl_site integer,
            osteo_knee_repl_age integer,
            d_cardiometabolic_risk_factors_kidney_thyroid_ra_complete integer,
            work_weekend integer,
            work_sedentary integer,
            work_vigorous integer,
            work_vigorous_days integer,
            work_vigorous_time integer, 
            work_vigorous_hours integer,
            work_vigorous_mins integer,
            work_vigorous_total integer,
            work_moderate integer,
            work_moderate_days integer,
            work_moderate_time integer,
            work_moderate_hours integer,
            work_moderate_mins integer,
            work_moderate_total integer, 
            work_day_total integer, 
            work_day_hours integer, 
            work_day_mins integer,
            transport_physical integer, 
            transport_physical_days integer,
            transport_physical_time integer,
            transport_physical_total integer, 
            transport_physical_hours integer,
            transport_physical_mins integer,
            leisure_physical integer,
            leisure_vigorous integer,
            leisure_vigorous_days integer,
            leisure_vigorous_time integer,
            leisure_vigorous_total integer, 
            leisure_vigorous_hours integer,
            leisure_vigorous_mins integer,
            leisure_moderate integer,
            leisure_moderate_days integer,
            leisure_moderate_time integer,
            leisure_moderate_total integer, 
            leisure_moderate_hours integer,
            leisure_moderate_mins integer,
            work_day_stng_total integer,  
            work_day_stng_hours integer,
            work_day_stng_mins integer,
            non_work_day_total integer, 
            non_work_day_hours integer,
            non_work_day_mins integer,
            week_sleep_time text,
            week_wakeup_time text,
            weekend_sleep_time text,
            weekend_wakeup_time text,
            sleep_room_pple_num integer, 
            sleep_room_livestock integer,
            sleep_on integer,
            mosquito_net_use integer,
            feel_alert integer, 
            sleeping_difficulty integer,
            difficulty_staysleep integer,
            waking_early_problem integer,
            waking_up_tired integer, 
            sleep_pattern_satis integer,
            sleep_interfere integer,
            mvpa_c integer,
            physical_activity_and_sleep_complete integer,
            standing_height numeric,
            weight numeric,
            waist_circumference_1 numeric, 
            waist_circumference_2 numeric, 
            waist_circumference numeric, 
            hip_circumference_1 numeric,
            hip_circumference_2 numeric,
            hip_circumference numeric,
            bmi_c numeric,
            bmi_cat_c text,
            waist_hip_r_c numeric,
            anthropometric_measurements_complete integer, 
            systolic_1 integer,
            diastolic_1 integer,
            pulse_1 integer,
            measurement_time_1 text,
            systolic_2 integer,
            diastolic_2 integer,
            pulse_2 integer,
            measurement_time_2 text,
            systolic_3 integer, 
            diastolic_3 integer,
            pulse_3 integer,
            measurement_time_3 text,
            bp_sys_average text,
            bp_dia_average text, 
            pulse_average text,
            blood_pressure_and_pulse_measurements_complete integer,
            vat_scat_measured integer,
            vat_scat_comment_ifnot text,
            pre_qc_visceral_fat numeric,
            pre_qc_subcutaneous_fat numeric,
            cimt_measured integer,
            cimt_comment_ifnot text,
            pre_qc_min_cimt_right numeric,
            pre_qc_max_cimt_right numeric,
            pre_qc_mean_cimt_right numeric,
            pre_qc_min_cimt_left numeric,
            pre_qc_max_cimt_left numeric,
            pre_qc_mean_cimt_left numeric,
            pre_qc_plaque_measured integer,
            plaque_comment_ifnot text,
            pre_qc_plaque_right_present integer,
            pre_qc_plaque_right numeric,
            pre_qc_plaque_left_present integer,
            pre_qc_plaque_left numeric,
            pre_qc_dxa_scan_completed integer,
            dxa_scan_comment_ifnot text,
            pre_qc_dxa_measurement_1 numeric,
            pre_qc_dxa_measurement_2 numeric,
            pre_qc_dxa_measurement_3 numeric,
            pre_qc_dxa_measurement_4 numeric,
            pre_qc_dxa_measurement_5 numeric,
            ultrasound_and_dxa_measurements_complete integer,
            breath_shortness integer,
            breath_shortness_ever integer,
            mucus integer,
            breath_too_short integer,
            cough integer,
            wheezing_whistling integer,
            asthma_diagnosed integer,
            age_diagnosed integer,
            asthma_treat_ever integer,
            asthma_treat_current integer,
            copd_suffer___1 integer,
            copd_suffer___2 integer,
            copd_suffer___3 integer,
            copd_suffer___0 integer,
            copd_suffer___9 integer,
            copd_treat integer,
            inhaled_medication integer,
            resp_meds_list text,
            puffs_time integer,
            puffs_times_day integer,
            measles_suffer___1 integer,
            measles_suffer___2 integer,
            measles_suffer___0 integer,
            measles_suffer___9 integer,
            a_respiratory_health_complete integer,
            major_surgery integer,
            chest_pain integer, 
            coughing_blood integer,
            acute_retinal_detach integer,
            any_pain_limit_blow integer, 
            diarrhea integer, 
            high_blood_pressure integer,
            diagnosed integer, -- naming conversion to change
            tb_treat_past4wks integer,
            no_resp_infection integer,
            infection___flu integer,
            infection___pneumonia integer,
            infection___bronchitis integer,
            infection___chest_cold integer,
            participation integer,
            wearing_tightclothes integer,
            wearing_dentures integer,
            participation_note integer, 
            participant_eligible_spiro integer,
            b_spirometry_eligibility_complete integer, 
            eligible_spiro_test integer,
            num_of_blows_spirometry integer,
            num_of_vblows_spirometry integer,
            pass_spirometry_test integer,
            comment_spirometry_test text,
            c_spirometry_test_complete integer,
            salb_administered_reversibility integer,
            salb_time_administered_reversibility text,
            time_started_spirometry_reversibility text,
            num_of_blows_reversibility integer,
            num_of_vblows_reversibility integer,
            comment_reversibility text,
            d_reversibility_test_complete integer,
            micr_take_antibiotics integer,
            micr_diarrhea_last_time integer,
            micr_worm_intestine_treat integer, 
            micr_probiotics_t_period integer, 
            micr_wormintestine_period integer,
            micr_probiotics_taken integer,
            a_microbiome_complete integer,
            last_eat_time text, -- to change to time
            last_ate_hrs numeric, 
            last_drink_time text,
            last_drink_hours numeric, 
            fasting_confirmed integer, 
            two_red_tubes_collected integer,
            red_tubes_num_ifno integer,
            one_purple_tube_collected integer,
            purple_tubes_num_ifno integer,
            one_grey_tube_collected integer, 
            grey_tubes_num_ifno integer,
            date_blood_taken text, --to change to date
            time_blood_taken text,
            b_blood_collection_complete integer,
            urine_collected integer,
            specify_reason_ifno_urinetest text,
            urine_container_batchnum text,
            urine_tube_expiry_date text,
            urine_taken_date text, --to change to date
            urinecollection_time text, --to change to time
            c_urine_collection_complete integer, 
            pre_qc_gluc_chol_test_conducted integer,
            comment_ifno_gluc_chol_test text,
            instrument_serialnum_gluc_chol text,
            gluc_chol_teststrip_batchnum text,
            gluc_chol_teststrip_expiry_date text, --its mixed, dates and options of dates... confusion
            pre_qc_gluc_chol_test_date text, -- to change to DATE
            pre_qc_gluc_chol_test_time text,
            pre_qc_glucose_test_result numeric,
            pre_qc_chol_result numeric,
            pre_qc_gluc_results_provided integer,
            pre_qc_gluc_results_notes text,
            pre_qc_chol_results_provided integer,
            pre_qc_chol_results_notes text,
            pre_qc_glucresults_discussed integer,
            pre_qc_cholresults_discussed integer,
            gluc_chol_seek_advice integer,
            hiv_test_conducted integer,
            comment_hiv_test_ifno text,
            hiv_pre_test_counselling integer,
            hiv_test_kit_serial_num text,
            hiv_strip_batch_num text,
            hiv_strip_expiry_date text,--its options of date
            hiv_test_date_done text, --to change toDATE
            result_hiv integer, 
            hiv_test_result_provided integer,
            post_test_counselling integer, 
            hivpositive_firsttime integer,
            hiv_seek_advice integer,
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
            completion_of_questionnaire_complete integer,
            glucose_result numeric,
            glucose_test_date text, -- tochange to date
            plasma_results_complete integer,
            date_serum_tested text, -- date
            s_creatinine numeric, 
            insulin_result numeric,	
            hdl numeric,
            friedewald_ldl_c numeric,
            ldl_measured numeric,
            cholesterol_1 numeric, 
            triglycerides numeric,
            non_hdl_c numeric, 
            dyslipidemia_c integer,
            egfr_c numeric,
            serum_results_complete integer,
            urine_batch text,
            urine_box text,
            date_urine_received text, --date
            ur_creatinine numeric, 
            urine_creatinine_test_date text, --date
            ur_albumin numeric,
            urine_albumin_test_date text, --date
            acr numeric,
            ur_protein numeric,
            urine_protein_test_date text, --date
            ckd_c integer,
            urine_results_complete integer,
            date_ultrasound_taken text, --date
            time_ultrasound_taken text, --time
            ultrasound_num_images text, 
            birfucations_comment text, 
            right_plaque_thickness numeric,
            left_plaque_thickness numeric,
            imt_valid integer,
            bifurcation_valid integer,
            ultrasound_rt_points integer,
            min_cimt_right numeric,
            max_cimt_right numeric,
            mean_cimt_right numeric,
            ultrasound_lt_points integer,
            min_cimt_left numeric,
            max_cimt_left numeric,
            mean_cimt_left numeric,
            visceral_fat numeric,
            subcutaneous_fat numeric,
            visceral_comment text,
            cimt_mean_max numeric,
            ultrasound_qc_results_complete integer  
        ) 
            '''
    return create_script