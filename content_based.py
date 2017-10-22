

'''
Document: Data preparation script for Content Based Filtering
Created Date: 14 Mar 2017
Author: Sai
Purpose : To prepare a file to rank similar products
 
--------
'''



import pandas as pd
import urllib2
import json
from pyspark.sql.types import StringType

from pyspark.sql import SQLContext
from pyspark import SparkContext
sc = SparkContext("local", "prod_api_feature_ec2")
sqlContext = SQLContext(sc)

full_data = []
 
# get all products from API
i=1
while True:
    url = 'https://btech.eg/rest/en/V1/products?searchCriteria[currentPage]='+str(i)+'&searchCriteria[pageSize]=500'
    #print i
    response = urllib2.urlopen(urllib2.Request(url, headers={'Authorization': 'Bearer xkkgxj33ib0uqs9cdcumo9an2g7th4gp'}))
    a = response.read()
    total = json.loads(a)['total_count']
    full_data = full_data + json.loads(a)['items']
    if (i*500 > total):
        break
    i = i + 1


# normalize JSON files
pd_df = pd.DataFrame()

for i in range(len(full_data)):
	for key in full_data[i].keys():
		if not isinstance(full_data[i][key], list):
			pd_df.loc[i, key] = full_data[i][key]
		else:
			if len(full_data[i][key]) != 0 and key == "custom_attributes":
				for j in range(len(full_data[i][key])):
					pd_df.loc[i, full_data[i][key][j]["attribute_code"]] = full_data[i][key][j]["value"]




pd_df = pd_df.drop('description', 1)
pd_df = pd_df.drop('meta_description', 1)
pd_df = pd_df.drop('meta_keyword', 1)

pd_df.to_csv("/home/ec2-user/content_based/prods_temp.csv",index=False,encoding='utf-8')

#call(["hdfs", "dfs", "-put", "./prods_temp.csv", "/"])

#call(["hdfs", "dfs", "-put", "./file1.csv", "/"])

# agreed features
##df = spark.read.csv("/home/ec2-user/content_based/file1.csv",header=True)
df = sqlContext.read.csv("/home/ec2-user/content_based/file1.csv",header=True)
dfCols = df.columns

# features not in API
newlists = ['Categories IDs', 'Categories Names', 'EN: Description', 'EN: Meta Description', 'EN: Meta Keyword', 'EN: Meta Title', \
'EN: Name', 'EN: Short Description', 'Images URLs', 'Price', 'SKU', 'Special Price', 'Status', 'Url Key', 'Youtube Video IDs', 'c', \
'gfk_electronic_cartridge_97567', 'gfk_filter_capacity_in_l_30029', 'gfk_short_description_40051']

# updating features
dfCols = list(set(dfCols) - set(newlists))

dfCols = dfCols + ['price', 'sku', 'special_price', 'status']


df1 = sqlContext.read.csv("/home/ec2-user/content_based/prods_temp.csv",header=True, encoding = 'utf-8', escape = "\"")

# features like height, width etc which have units
unitNumCols = ['gfk_width_in_mm_43926', 'gfk_width_in_cm_57282', 'gfk_width_in_cm_29502', \
'gfk_width_in_cm_18673', 'gfk_weight_in_kg_34741', 'gfk_weight_in_grams_79305', \
'gfk_on_mode_power_consum_40129', 'gfk_number_of_airflow_se_33881', 'gfk_maximum_wattage_98571', \
'gfk_maximum_loading_capa_10240', 'gfk_height_in_mm_59064', 'gfk_height_in_cm_66358', \
'gfk_display_size_in_inch_82970', 'gfk_depth_in_cm_74644']


binCols = ['auto_focus', 'bluetooth', 'built_in_flash', 'face_detection_mode', \
'image_stabilizer', 'installments_allow', 'manual_white_balance', 'multi_shot_camera', \
'picture_mode', 'sw_featured', 'touchscreen', 'white_balance', 'wifi_based']

# category features
catCols = ['filters_refrigerator', 'gfk_0_cooling_zone_90316', 'gfk_1080p_upscaling_32541', \
'gfk_3d_34114', 'gfk_3d_technology_66634', 'gfk_additional_shaving_h_26828', \
'gfk_adjustable_guide_com_10208', 'gfk_adjustable_height_35902', 'gfk_air_purification_18382', \
'gfk_air_ventilator_55358', 'gfk_airplay_53995', 'gfk_anticalc_filter_80610', \
'gfk_application_store_33901', 'gfk_aroma_switch_40041', 'gfk_aspect_ratio_27119', \
'gfk_automatic_90056', 'gfk_automatic_dosage_sys_24071', 'gfk_automatic_program_53929', \
'gfk_automatic_shut_off_20467', 'gfk_automatic_shut_off_82934', 'gfk_bagless_technology_15139', \
'gfk_bluetooth_52764', 'gfk_boil_dry_protection_91042', 'gfk_brand_of_graphic_chi_32804', \
'gfk_brush_included_36822', 'gfk_built_in_camera_43384', 'gfk_built_in_refrigerato_76139', \
'gfk_capacity_of_bean_con_71012', 'gfk_cavity_material_53196', 'gfk_cd_player_recorder_85296', \
'gfk_ceramic_coating_96473', 'gfk_chimney_decorative_t_98165', 'gfk_cold_wash_program_15983', \
'gfk_compatible_with_pad__14097', 'gfk_construction_39367', 'gfk_construction_70698', \
'gfk_construction_99396', 'gfk_construction_type_27410', 'gfk_continous_steam_20067', \
'gfk_control_of_drying_43711', 'gfk_controls_in_handle_64686', 'gfk_cool_wall_64102', \
'gfk_coolwall_57396', 'gfk_cup_warmer_32573', 'gfk_curved_design_70411', \
'gfk_defrost_function_98744', 'gfk_dehumidifier_functio_42158', 'gfk_depth_of_front_speak_57985', \
'gfk_diameter_in_inches_38495', 'gfk_different_temperatur_35880', 'gfk_diffuser_30275', \
'gfk_digital_radio_58153', 'gfk_display_81216', 'gfk_display_of_remaining_51893', \
'gfk_display_technology_45287', 'gfk_dolby_dts_24472', 'gfk_drying_capacity_in_k_49019', \
'gfk_drying_function_47384', 'gfk_drying_time_for_dry__62944', 'gfk_dvd_audio_playback_76301', \
'gfk_dvd_player_recorder_85544', 'gfk_dvd_r_recording_38620', 'gfk_dvd_r_recording_95250', \
'gfk_dvd_ram_recording_85417', 'gfk_dvd_rw_recording_81227', 'gfk_dvd_rw_recording_92400', \
'gfk_dvd_writer_87213', 'gfk_electronic_sensor_99562', 'gfk_embedded_3g_4g_75924', \
'gfk_extraction_system_52754', 'gfk_extradeep_slots_11343', 'gfk_fan_convection_96207', \
'gfk_filter_system_44173', 'gfk_filter_system_77722', 'gfk_fingerprint_reader_77174', \
'gfk_floating_hinges_49974', 'gfk_form_factor_81714', 'gfk_frame_material_19903', \
'gfk_freezer_position_40257', 'gfk_freezer_stars_77282', 'gfk_front_decoration_79720', \
'gfk_frying_without_oil_60329', 'gfk_fuel_of_hob_67400', 'gfk_full_body_stainless__96220', \
'gfk_full_hd_99016', 'gfk_full_nofrost_system_77476', 'gfk_gpu_model_graphic_ca_53568', \
'gfk_grill_function_45613', 'gfk_grill_plates_26215', 'gfk_grilling_plate_grid_40919', \
'gfk_half_load_57562', 'gfk_hdmi_46757', 'gfk_hdmi_standard_67082', 'gfk_hdtv_48626', \
'gfk_heating_function_92872', 'gfk_height_in_mm_99956', 'gfk_height_of_front_spea_84774', \
'gfk_home_bar_34028', 'gfk_housing_material_23408', 'gfk_housing_material_94384', \
'gfk_housing_material_99849', 'gfk_ice_cube_dispenser_89692', 'gfk_integrated_12753', \
'gfk_integrated_coffee_gr_92823', 'gfk_internet_radio_89750', 'gfk_ionic_technology_61729', \
'gfk_knob_control_18698', 'gfk_led_backlight_29130', 'gfk_lift_system_47003', \
'gfk_maximum_capacity_in__82606', 'gfk_maximum_volume_in_cu_91733', \
'gfk_maximum_volume_in_li_84252', 'gfk_maximum_wattage_49531', 'gfk_maximum_wattage_66751', \
'gfk_maximum_wattage_90217', 'gfk_maximum_wattage_96585', 'gfk_memory_card_slot_22188', \
'gfk_milk_foamer_33759', 'gfk_mounting_15005', 'gfk_mp3_playback_89773', \
'gfk_nofrost_system_22591', 'gfk_nose_ear_trimmer_att_88778', 'gfk_number_of_cooking_zo_70438', \
'gfk_number_of_detachable_85002', 'gfk_number_of_different__63294', 'gfk_number_of_doors_69718', \
'gfk_number_of_electric_r_75348', 'gfk_number_of_fanspeed_s_42048', 'gfk_number_of_guide_comb_46506', \
'gfk_number_of_hdmi_input_95233', 'gfk_number_of_motors_56815', 'gfk_number_of_place_sett_70611', \
'gfk_number_of_power_step_45607', 'gfk_number_of_slices_14262', 'gfk_number_of_speakers_94939', \
'gfk_number_of_speed_sett_38885', 'gfk_number_of_speed_sett_61120', 'gfk_number_of_temperatur_49803', \
'gfk_number_of_usb_ports_32773', 'gfk_operating_system_ver_16172', 'gfk_oscillation_92606', \
'gfk_pad_capsules_system_16674', 'gfk_phone_function_19370', 'gfk_power_capacity_contr_89485', \
'gfk_power_supply_67603', 'gfk_power_supply_88796', 'gfk_pressure_system_67947', \
'gfk_processor_cores_12281', 'gfk_product_type_53552', 'gfk_product_type_62791', \
'gfk_reception_standard_17324', 'gfk_reheat_function_38244', 'gfk_remote_control_20053', \
'gfk_removable_tank_37464', 'gfk_rms_wattage_36480', 'gfk_safety_automatic_sto_95648', \
'gfk_sandwich_plates_34729', 'gfk_separate_temperature_91066', 'gfk_shape_69559', 'gfk_shape_95614', \
'gfk_shelves_material_43665', 'gfk_shortage_alarm_34871', 'gfk_sloping_plate_97608', \
'gfk_smart_connect_60153', 'gfk_speed_function_22631', 'gfk_start_delay_option_40672', \
'gfk_steam_heat_up_time_i_46798', 'gfk_steam_shot_36326', 'gfk_straightener_55980', \
'gfk_super_video_cd_video_93947', 'gfk_surface_61517', 'gfk_technology_83817', 'gfk_technology_83817', \
'gfk_temperature_display_26106', 'gfk_thermostat_64645', 'gfk_time_control_27745', 'gfk_timer_11930', 'gfk_timer_40127', \
'gfk_timer_61087', 'gfk_timer_68815', 'gfk_tong_included_15852', 'gfk_top_bottom_heating_59736', \
'gfk_touch_control_80817', 'gfk_true_resolution_90353', 'gfk_tube_material_90812', \
'gfk_turntable_35151', 'gfk_type_10316', 'gfk_type_14250', 'gfk_type_19281', 'gfk_type_70681', \
'gfk_type_96825', 'gfk_type_97037', 'gfk_type_97904', 'gfk_type_of_dust_contain_63126', \
'gfk_type_of_electronic_i_53128', 'gfk_type_of_espresso_mac_61306', 'gfk_type_of_freezer_32975', \
'gfk_type_of_grinder_37846', 'gfk_type_of_hood_10011', 'gfk_type_of_jug_62271', \
'gfk_type_of_loading_32264', 'gfk_type_of_steamer_56838', 'gfk_type_shape_17593', \
'gfk_upnp_76955', 'gfk_usb_43063', 'gfk_usb_interface_32063', 'gfk_usb_recording_46886', \
'gfk_variable_slot_width_66448', 'gfk_variable_steam_78175', 'gfk_video_resolution_in__53848', \
'gfk_volumizer_75299', 'gfk_waffle_plates_76247', 'gfk_warming_rack_60695', 'gfk_washable_99472', \
'gfk_water_control_system_23999', 'gfk_water_dispenser_92277', 'gfk_water_filter_64620', \
'gfk_water_level_info_53052', 'gfk_water_level_info_99818', 'gfk_waterfilled_drip_tra_58051', \
'gfk_web_cam_92971', 'gfk_web_content_access_86941', 'gfk_weight_in_kg_62430', 'gfk_wet_dry_use_77927', \
'gfk_wet_hair_use_46446', 'gfk_width_of_front_speak_54044', 'gfk_wifi_connectivity_91335', \
'gfk_wireless_speakers_38681', 'horse_power', 'interest_rate', 'status']

numCols = ['gfk_clock_speed_in_mhz_76510', 'gfk_depth_in_mm_82347', 'gfk_energy_consumption_p_26722', \
'gfk_etilize_key_69270', 'gfk_net_capacity_freezer_82220', 'gfk_net_capacity_in_litr_23606', \
'gfk_net_capacity_refrige_39668', 'gfk_net_capacity_total_d_57682', 'gfk_photo_resolution_hei_41140', \
'gfk_water_consumption_pe_32089', 'price', 'special_price', 'gfk_maximum_air_flow_rat_13922', 'gfk_maximum_wattage_82526']




reqd_df = df1.select(dfCols)


strCols = list(set(reqd_df.columns) - set(numCols) - set(binCols) - set(catCols)) 


strCols = list(set(strCols) - set(['sku']))

#convert to Float type
from pyspark.sql.types import FloatType
for colName in numCols:
    try:
        reqd_df = reqd_df.withColumn(colName, reqd_df[colName].cast(FloatType()))
    except:
        pass


#Fill zeros in numerical columns
reqd_df = reqd_df.fillna(value=0,subset=numCols)

reqd_df.createOrReplaceTempView('reqd_df')
reqd_df = sqlContext.sql("SELECT *, ((price - special_price)/price) AS discount from reqd_df")
##reqd_df = spark.sql("SELECT *, ((price - special_price)/price) AS discount from reqd_df")

reqd_df = reqd_df.drop('special_price')

numCols.remove('special_price')

reqd_df = reqd_df.fillna(value=0,subset='discount')

#remove units for width, depth columns
def checkNum(y):
	if y is not None:
		z = str(y.encode('ascii', 'ignore'))
		return filter(lambda x: x.isdigit(), z)

from pyspark.sql.functions import udf

checkNumUDF = udf(checkNum, StringType())

for column in unitNumCols:
	reqd_df = reqd_df.withColumn(column, checkNumUDF(reqd_df[column]))




# MinMax scaling  value - min/(max - min)

from pyspark.sql.functions import min, max
for colName in numCols + unitNumCols:   
    try:
        minVal, maxVal= reqd_df.select(min(colName), max(colName)).first()
        if (maxVal - minVal > 0):
        	new_name = colName + "_scaled"
        	reqd_df.createOrReplaceTempView('reqd_df')
        	reqd_df = sqlContext.sql("SELECT *, (({}-{})/({}-{})) AS {} from reqd_df".format(colName, minVal, maxVal, minVal, new_name))
        	reqd_df = reqd_df.drop(colName)
    except:
    	pass



# Remove special characters and white spaces
def checkAlNum(y):
	if y is not None:
		z = str(y.encode('ascii', 'ignore'))
		return filter(lambda x: x.isalnum(), z)


checkAlNumUDF = udf(checkAlNum, StringType())

for column in strCols+catCols:
	reqd_df = reqd_df.withColumn(column, checkAlNumUDF(reqd_df[column]))



reqd_pd_df2 = reqd_df.toPandas()

for column in strCols+catCols:
	reqd_pd_df2[column] = reqd_pd_df2[column].replace(to_replace = 'Unknown')
	reqd_pd_df2[column] = reqd_pd_df2[column].replace(to_replace = 'NotApplicable')



# One hot encoding using pandas
reqd_pd_df3 = pd.get_dummies(reqd_pd_df2, dummy_na=False, columns=strCols+catCols+binCols)


reqd_pd_df3.to_csv("/home/ec2-user/content_based/product_features.csv",index=False,encoding='utf-8', header=False)

#call(["hdfs", "dfs", "-put", "./product_features.csv", "/"])

item_feat = pd.read_csv("/home/ec2-user/content_based/product_features.csv", header=None)

header = item_feat.iloc[1:,0]

feat_clean = item_feat.drop(item_feat.columns[[0,1,2,4]], axis=1)

#print(header)

item_matrix = feat_clean.iloc[1:,1:]


from sklearn.metrics.pairwise import cosine_similarity as cs
item_sim = cs(item_matrix)

item_sim = pd.DataFrame(item_sim,index=header, columns=header)
#item_sim.to_csv('item_sim.csv')

#Return top n matches

def sim_items(SKU,n):
    return item_sim[SKU].sort_values(ascending=False)[1:n+1]



#print(sim_items('1TEFRACTIFAH95029L20',10))
#print(sim_items('1ESDFH024000C2015L29',10))



# Create JSON file
results = {}
for sku in header:
    cat = {}
    i = 1
    for item in sim_items(sku,10).to_frame().index:
        cat[item] = i
        i = i + 1
    results[sku] = cat




with open('/home/ec2-user/content_based/items_similar.json', 'w') as outfile:
    json.dump(results, outfile)


# Write the file to S3
import boto
import boto.s3
import os
from boto.s3.key import Key

AWS_ACCESS_KEY_ID = 'AKIAJ3QQJCR5ACLVKQRA'
AWS_SECRET_ACCESS_KEY = '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'


conn = boto.connect_s3(AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY)

bucket = conn.get_bucket('bteckenodes')
key_name = 'items_similar.json'
path = 'content_based' 

file_path = '/home/ec2-user/content_based/items_similar.json'

full_key_name = os.path.join(path, key_name)
k = bucket.new_key(full_key_name)
k.set_contents_from_filename(file_path)
k.make_public()

key_name = 'product_features.csv'
path = 'content_based' 

file_path = '/home/ec2-user/content_based/product_features.csv'

full_key_name = os.path.join(path, key_name)
k = bucket.new_key(full_key_name)
k.set_contents_from_filename(file_path)
k.make_public()

