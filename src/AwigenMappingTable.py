import pandas as pd
import numpy as np

outputDir = './resources/Mapping_Table/'

# 1. Phenotype data
awigen1_phenos_df = pd.read_csv(outputDir + 'input_all_sites_v2.5.3.24.csv', low_memory=False)
awigen1_phenos_df = awigen1_phenos_df[['study_id', 'site', 'sex']]

awigen2_phenos_df = pd.read_csv(outputDir + 'input_all_data_20220330_update.csv', low_memory=False, sep=';')
awigen2_phenos_df = awigen2_phenos_df[['study_id', 'gene_site', 'demo_gender']]

merge_df = pd.merge(awigen1_phenos_df, awigen2_phenos_df, indicator=True, how='outer', left_on='study_id', right_on='study_id', suffixes=('_left', '_right'))
merge_df.rename(columns={"_merge": "phenotypes"}, inplace=True)

# 2. DNA Samples
awigen_dna_df = pd.read_csv(outputDir + 'input_dna_table.csv', sep=';')

awigen_dna_df = awigen_dna_df[['Participant ID']]
awigen_dna_df.rename(columns={"Participant ID": "study_id"}, inplace=True)
awigen_dna_df['study_id'] = awigen_dna_df['study_id'].str.split(':').str[0]
awigen_dna_df['study_id'] = awigen_dna_df['study_id'].str.strip()
awigen_dna_df['study_id'].replace('', np.nan, inplace=True)
awigen_dna_df = awigen_dna_df.dropna()

dna_duplicates_df = awigen_dna_df['study_id'].value_counts()
dna_duplicates_df = dna_duplicates_df[dna_duplicates_df > 1]
dna_duplicates_df = dna_duplicates_df.reset_index(level=0)
dna_duplicates_df = dna_duplicates_df[['index']]
dna_duplicates_df.rename(columns={"index": "study_id"}, inplace=True)

awigen_dna_df = awigen_dna_df.drop_duplicates()

merge_df = pd.merge(merge_df, awigen_dna_df, indicator=True, how='outer', left_on='study_id', right_on='study_id', suffixes=('_left', '_right'))
merge_df.rename(columns={"_merge": "dna_samples"}, inplace=True)

merge_df = pd.merge(merge_df, dna_duplicates_df, indicator=True, how='outer', left_on='study_id', right_on='study_id', suffixes=('_left', '_right'))
merge_df.rename(columns={"_merge": "dna_duplicates"}, inplace=True)

# 3. Genotyped samples (successful)
awigen1_genotyped_df = pd.read_csv(outputDir + 'input_awigen1_genotyped.csv', names=['study_id'])

merge_df = pd.merge(merge_df, awigen1_genotyped_df, indicator=True, how='outer', left_on='study_id', right_on='study_id', suffixes=('_left', '_right'))
merge_df.rename(columns={"_merge": "genotyped_samples_successful"}, inplace=True)

# 4. Genotyped samples (problematic)
awigen1_genotyped_prob_df = pd.read_csv(outputDir + 'input_awigen_problematic.csv', names=['study_id'])

merge_df = pd.merge(merge_df, awigen1_genotyped_prob_df, indicator=True, how='outer', left_on='study_id', right_on='study_id', suffixes=('_left', '_right'))
merge_df.rename(columns={"_merge": "genotyped_samples_problematic"}, inplace=True)

# 5. WGS Samples (SA)
awigen_wgs_df = pd.read_csv(outputDir + 'input_awi_100_info.csv', sep=';')
awigen_wgs_df = awigen_wgs_df[['ID']]
awigen_wgs_df.rename(columns={"ID": "study_id"}, inplace=True)

merge_df = pd.merge(merge_df, awigen_wgs_df, indicator=True, how='outer', left_on='study_id', right_on='study_id', suffixes=('_left', '_right'))
merge_df.rename(columns={"_merge": "wgs_samples_sa"}, inplace=True)

# 6. WGS Samples (Baylor)
awigen_wgs_df = pd.read_csv(outputDir + 'input_Baylor_west_ids.csv', sep=';', names=['study_id'])

merge_df = pd.merge(merge_df, awigen_wgs_df, indicator=True, how='outer', left_on='study_id', right_on='study_id', suffixes=('_left', '_right'))
merge_df.rename(columns={"_merge": "wgs_samples_baylor"}, inplace=True)

# 7. AWIGen Population Study
awigen_pop_study_df = pd.read_csv(outputDir + 'input_AWIGenPopulationStudy.csv', low_memory=False, sep=';')
awigen_pop_study_df = awigen_pop_study_df[['awi_number', 'site']]
awigen_pop_study_df.rename(columns={"awi_number": "study_id", "site": "site_pop_study"}, inplace=True)
awigen_pop_study_df['study_id'] = awigen_pop_study_df['study_id'].str.upper()

merge_df = pd.merge(merge_df, awigen_pop_study_df, indicator=True, how='outer', left_on='study_id', right_on='study_id', suffixes=('_left', '_right'))
merge_df.rename(columns={"_merge": "awigen_pop_study"}, inplace=True)

# 8. Variant bio
variant_bio_df = pd.read_csv(outputDir + 'input_variant_bio.csv', low_memory=False, sep=';')
variant_bio_df = variant_bio_df[['study_id']]

variant_bio_agincourt_df = pd.read_csv(outputDir + 'input_variant_bio_agincourt.csv', low_memory=False, sep=';')
variant_bio_agincourt_df = variant_bio_agincourt_df[['study_id']]

variant_bio_df2 = pd.concat([variant_bio_df, variant_bio_agincourt_df], ignore_index=True)
variant_bio_df2 = variant_bio_df2.drop_duplicates()

merge_df = pd.merge(merge_df, variant_bio_df2, indicator=True, how='outer', left_on='study_id', right_on='study_id', suffixes=('_left', '_right'))
merge_df.rename(columns={"_merge": "variant_bio"}, inplace=True)

# 9. Microbiome
microbiome_df = pd.read_csv(outputDir + 'input_micro_all.csv', low_memory=False, sep=';', names=['study_id'])
microbiome_df = microbiome_df[['study_id']]
microbiome_df = microbiome_df.drop_duplicates()

merge_df = pd.merge(merge_df, microbiome_df, indicator=True, how='outer', left_on='study_id', right_on='study_id', suffixes=('_left', '_right'))
merge_df.rename(columns={"_merge": "microbiome"}, inplace=True)

# 10. H3A Samples
h3a_samples_awigen_df = pd.read_csv(outputDir + 'input_h3a_samples_awigen.csv', low_memory=False, sep=';')
h3a_samples_awigen_df = h3a_samples_awigen_df[['DE IDENTIFIED PARTICIPANT ID']]
h3a_samples_awigen_df.rename(columns={"DE IDENTIFIED PARTICIPANT ID": "study_id"}, inplace=True)
# h3a_samples_awigen_df['study_id'].value_counts()
h3a_samples_awigen_df = h3a_samples_awigen_df.drop_duplicates()

merge_df = pd.merge(merge_df, h3a_samples_awigen_df, indicator=True, how='outer', left_on='study_id', right_on='study_id', suffixes=('_left', '_right'))
merge_df.rename(columns={"_merge": "h3a_samples_awigen1"}, inplace=True)

h3a_samples_awigen2_df = pd.read_csv(outputDir + 'input_h3a_samples_awigen2.csv', low_memory=False, sep=';')
h3a_samples_awigen2_df = h3a_samples_awigen2_df[['DE IDENTIFIED PARTICIPANT ID']]
h3a_samples_awigen2_df.rename(columns={"DE IDENTIFIED PARTICIPANT ID": "study_id"}, inplace=True)
# h3a_samples_awigen2_df['study_id'].value_counts()
h3a_samples_awigen2_df = h3a_samples_awigen2_df.drop_duplicates()

merge_df = pd.merge(merge_df, h3a_samples_awigen2_df, indicator=True, how='outer', left_on='study_id', right_on='study_id', suffixes=('_left', '_right'))
merge_df.rename(columns={"_merge": "h3a_samples_awigen2"}, inplace=True)

# 11. Spare buffy coats Phase 1
spare_buffy_awigen1_df = pd.read_csv(outputDir + 'input_buffy_coats_awigen1.csv', low_memory=False, sep=';')
spare_buffy_awigen1_df = spare_buffy_awigen1_df[['SUBJECTUID']]
spare_buffy_awigen1_df.rename(columns={"SUBJECTUID": "study_id"}, inplace=True)
spare_buffy_awigen1_df = spare_buffy_awigen1_df.drop_duplicates()

merge_df = pd.merge(merge_df, spare_buffy_awigen1_df, indicator=True, how='outer', left_on='study_id', right_on='study_id', suffixes=('_left', '_right'))
merge_df.rename(columns={"_merge": "spare_buffy_awigen1"}, inplace=True)

# 12. Spare buffy coats Phase 2
spare_buffy_awigen2_df = pd.read_csv(outputDir + 'input_phase2_buffies.csv', low_memory=False, sep=';')
spare_buffy_awigen2_df = spare_buffy_awigen2_df[['study_id']]
spare_buffy_awigen2_df = spare_buffy_awigen2_df.drop_duplicates()

merge_df = pd.merge(merge_df, spare_buffy_awigen2_df, indicator=True, how='outer', left_on='study_id', right_on='study_id', suffixes=('_left', '_right'))
merge_df.rename(columns={"_merge": "spare_buffy_awigen2"}, inplace=True)

# 13. Raylton serum
raylton_serum_awigen1_df = pd.read_csv(outputDir + 'input_raylton_serum_awigen1.csv', low_memory=False, sep=';')
raylton_serum_awigen1_df = raylton_serum_awigen1_df[['ID']]
raylton_serum_awigen1_df.rename(columns={"ID": "study_id"}, inplace=True)
raylton_serum_awigen1_df = raylton_serum_awigen1_df.drop_duplicates()

merge_df = pd.merge(merge_df, raylton_serum_awigen1_df, indicator=True, how='outer', left_on='study_id', right_on='study_id', suffixes=('_left', '_right'))
merge_df.rename(columns={"_merge": "raylton_serum_awigen1"}, inplace=True)

raylton_serum_awigen2_df = pd.read_csv(outputDir + 'input_raylton_serum_awigen2.csv', low_memory=False, sep=';')
raylton_serum_awigen2_df = raylton_serum_awigen2_df[['Participant ID']]
raylton_serum_awigen2_df.rename(columns={"Participant ID": "study_id"}, inplace=True)
raylton_serum_awigen2_df = raylton_serum_awigen2_df.drop_duplicates()

merge_df = pd.merge(merge_df, raylton_serum_awigen2_df, indicator=True, how='outer', left_on='study_id', right_on='study_id', suffixes=('_left', '_right'))
merge_df.rename(columns={"_merge": "raylton_serum_awigen2"}, inplace=True)

# 14. HAALSI Study
haalsi_df = pd.read_csv(outputDir + 'input_HAALSI_study_ids.csv', low_memory=False, sep=';')
haalsi_df = haalsi_df[['Global Spec ID']]
haalsi_df.rename(columns={'Global Spec ID': "study_id"}, inplace=True)
haalsi_df['study_id'] = haalsi_df['study_id'].str.slice(0,5)

# haalsi_df['study_id'].value_counts()
haalsi_df = haalsi_df.drop_duplicates()

merge_df = pd.merge(merge_df, haalsi_df, indicator=True, how='outer', left_on='study_id', right_on='study_id', suffixes=('_left', '_right'))
merge_df.rename(columns={"_merge": "haalsi"}, inplace=True)

# 15. Long read samples
long_read_df = pd.read_csv(outputDir + 'input_long_read_from_laura.csv', low_memory=False, sep=';')
long_read_df = long_read_df[['study_id']]

merge_df = pd.merge(merge_df, long_read_df, indicator=True, how='outer', left_on='study_id', right_on='study_id', suffixes=('_left', '_right'))
merge_df.rename(columns={"_merge": "long_read_Laura"}, inplace=True)

# 16. Natalie samples
natalie_samples_df = pd.read_csv(outputDir + 'input_Natalie_samples.csv', low_memory=False, sep=';')
natalie_samples_df = natalie_samples_df[['study_id']]

merge_df = pd.merge(merge_df, natalie_samples_df, indicator=True, how='outer', left_on='study_id', right_on='study_id', suffixes=('_left', '_right'))
merge_df.rename(columns={"_merge": "natalie_samples"}, inplace=True)

out_df = merge_df
out = pd.DataFrame()
out['study_id'] = out_df['study_id']

out['site'] = out_df['site']
out['site'][out_df['gene_site'].notna()] = out_df['gene_site'][out_df['gene_site'].notna()]
out['site'][out_df['site_pop_study'].notna()] = out_df['site_pop_study'][out_df['site_pop_study'].notna()]

out['awigen1_phenotypes'] = out_df['site'].notna().astype(int)
out['awigen2_phenotypes'] = out_df['gene_site'].notna().astype(int)
out['awigen_phenotypes'] = ((out['awigen1_phenotypes']) | (out['awigen2_phenotypes'])).astype(int)

out['awigen2_not_awigen1'] = ((out['awigen1_phenotypes'] == 0) & (out['awigen2_phenotypes'] == 1)).astype(int)

out['awigen_genotyped_successful'] = ((out_df['genotyped_samples_successful'] == 'both') | (out_df['genotyped_samples_successful'] == 'right_only')).astype(int)
out['awigen_genotyped_problematic'] = ((out_df['genotyped_samples_problematic'] == 'both') | (out_df['genotyped_samples_problematic'] == 'right_only')).astype(int)

out['awigen_dna'] = ((out_df['dna_samples'] == 'both') | (out_df['dna_samples'] == 'right_only')).astype(int)

out['awigen_dna_duplicated'] = ((out_df['dna_duplicates'] == 'both') | (out_df['dna_duplicates'] == 'right_only')).astype(int)

out['awigen_wgs_sa'] = (out_df['wgs_samples_sa'] == 'both').astype(int)
out['awigen_wgs_baylor'] = (out_df['wgs_samples_baylor'] == 'both').astype(int)

out['awigen_pop_study'] = ((out_df['awigen_pop_study'] == 'both') | (out_df['awigen_pop_study'] == 'right_only')).astype(int)

out['awigen_phenotypes_or_pop_study'] = ((out['awigen_phenotypes']) | (out['awigen_pop_study'])).astype(int)

out['variant_bio'] = ((out_df['variant_bio'] == 'both') | (out_df['variant_bio'] == 'right_only')).astype(int)

out['haalsi'] = ((out_df['haalsi'] == 'both') | (out_df['haalsi'] == 'right_only')).astype(int)

out['microbiome'] = ((out_df['microbiome'] == 'both') | (out_df['microbiome'] == 'right_only')).astype(int)

out['h3a_samples_awigen1'] = ((out_df['h3a_samples_awigen1'] == 'both') | (out_df['h3a_samples_awigen1'] == 'right_only')).astype(int)
out['h3a_samples_awigen2'] = ((out_df['h3a_samples_awigen2'] == 'both') | (out_df['h3a_samples_awigen2'] == 'right_only')).astype(int)

out['spare_buffy_awigen1'] = ((out_df['spare_buffy_awigen1'] == 'both') | (out_df['spare_buffy_awigen1'] == 'right_only')).astype(int)
out['spare_buffy_awigen2'] = ((out_df['spare_buffy_awigen2'] == 'both') | (out_df['spare_buffy_awigen2'] == 'right_only')).astype(int)
out['spare_buffy'] = ((out['spare_buffy_awigen1']) | (out['spare_buffy_awigen2'])).astype(int)

out['raylton_serum_awigen1'] = ((out_df['raylton_serum_awigen1'] == 'both') | (out_df['raylton_serum_awigen1'] == 'right_only')).astype(int)
out['raylton_serum_awigen2'] = ((out_df['raylton_serum_awigen2'] == 'both') | (out_df['raylton_serum_awigen2'] == 'right_only')).astype(int)

out['long_read_Laura'] = ((out_df['long_read_Laura'] == 'both') | (out_df['long_read_Laura'] == 'right_only')).astype(int)

out['natalie_samples'] = ((out_df['natalie_samples'] == 'both') | (out_df['natalie_samples'] == 'right_only')).astype(int)

out = out[['study_id', 'site',
           'awigen1_phenotypes', 'awigen2_phenotypes', 'awigen_phenotypes', 'awigen2_not_awigen1',
           'spare_buffy_awigen1', 'spare_buffy_awigen2', 'spare_buffy',
           'awigen_dna', 'awigen_dna_duplicated',
           'awigen_genotyped_successful', 'awigen_genotyped_problematic',
           'awigen_pop_study', 'awigen_phenotypes_or_pop_study',
           'variant_bio', 'haalsi',
           'awigen_wgs_sa', 'awigen_wgs_baylor',
           'microbiome',
           'h3a_samples_awigen1', 'h3a_samples_awigen2',
           'raylton_serum_awigen1', 'raylton_serum_awigen2',
           'long_read_Laura', 'natalie_samples']]

# Remove unknown DNA samples
out2 = out.drop(out.loc[(out.sum(axis=1) == 1) & (out['awigen_dna']==1)].index)
out2 = out2.drop(out2.loc[(out2.sum(axis=1) == 2) & (out2['awigen_dna']==1) & (out2['awigen_dna_duplicated']==1)].index)

out2.to_csv(outputDir + 'awigen_mapping_table.csv')