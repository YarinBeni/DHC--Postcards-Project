import pandas as pd
import os
from PIL import Image

# # todo: 1)ask if ID is Divider number -
# #  A: No Useless column DIVIDER NUM is the one
# #  2) ask if need to add acronym column to big excel and in each small excel for every divider we dont need
# #  A: decided on number for publisher instead of acronym
# #  4) every row for every image ? or every row for both F and B images ( if so need to merge relevant data)
# #  A: every row for 2 images F&B in the folder
# #  5) what to do with duplicates rows ? right now i removed them
# #  A: they are not duplicates they are different samples of the same postcard
# # todo: in next meeting ask to add F / B to row or how to mange it with yael
#  todo: A: every row is connected to two images one with F and one with B


PREFIX_PATH = r"C:\Users\yarin\PycharmProjects\DHC__project_postcards"

MATRIX_PATH = r"C:\Users\yarin\PycharmProjects\DHC__project_postcards\shared_via_googlecloud\AMERICAN COLONY - SCOTS " \
              r"MISSION\DATA_TABLE.P063. -- SCOUT MISSION.xlsx "

POSTCARDS_PATH = r"\shared_via_googlecloud\AMERICAN COLONY - SCOTS MISSION"

DATABASE_PATH = r"C:\Users\yarin\PycharmProjects\DHC__project_postcards\shared_via_googlecloud\DB"


def preprocess_dataset(path):
    """
    load excel table as dataframe object with following preprocess steps:
    1) replace columns name " " with "_"
    2) make new column name Identifier by by template P[0-9]{5}\.D[0-9]{3}\.PN[0-9]{5}\.S[0-9]{3}\.[FR]
    :param path: string,absolute path to the excel
    :return df: dataframe
    """

    df = pd.read_excel(path)

    # columns name with "_" instead of " "
    df.columns = df.columns.str.replace(' ', '_')

    # make serial number for each postcard if there is duplicates
    df["SERIAL"] = 0
    names = df["POSTCARD_NUM"].unique()
    for i in range(len(names)):
        count = 1
        for j, row in df.iterrows():
            if row["POSTCARD_NUM"] == names[i]:
                df.at[j, "SERIAL"] = count
                count += 1
        count = 1

    # make Identifier column by template P[0-9]{3}\.D[0-9]{3}\.PN[0-9]{5}\.S[0-9]{3}\.[FR]
    # example: 002.D012.PN00001.S002.F
    # under the assumption PUB_NUM is natural number

    #  NOW not adding F and B at end its added in rename_by_identifier
    AMOUNT_ZEROS_DIVIDER = AMOUNT_ZEROS_SERIAL = 3
    AMOUNT_ZEROS_PUBLISHER = AMOUNT_ZEROS_POSTCARD = 5
    df['IDENTIFIER'] = df.apply(
        lambda row: "P" + (AMOUNT_ZEROS_PUBLISHER - len(str(row.PUB_NUM))) * "0" + str(row.PUB_NUM) +
                    ".D" + (AMOUNT_ZEROS_DIVIDER - len(str(row.DIVIDER_NUM))) * "0" + str(row.DIVIDER_NUM) +
                    ".PN" + (AMOUNT_ZEROS_POSTCARD - len(str(row.POSTCARD_NUM))) * "0" + str(row.POSTCARD_NUM) +
                    ".S" + (AMOUNT_ZEROS_SERIAL - len(str(row.SERIAL))) * "0" + str(row.SERIAL), axis=1)
    return df


def rename_by_identifier(dirpath, filesname, df):
    """
    receives a path to folder holding images, front and back for each unique sample (by postcard number) and rename it
    to the name constructed in column name Identifier
    Under the assumptions
    1)every row has Identifier value string type and every image with same postcard number has Identifier in the row
    2)every image in folder ends with B or F (made by scanner) and both exits
    3)the order of the images in folder is pair ascending ,1F/B ,1F/B ,2F/B ,2F/B  ,3F/B, 3F/B ...
    :param data_path: string,absolute path to images folder where each image has F and B image
    :param df: dataframe, with  Identifier column with future name for each image in the path
    """
    TIF_LABEL, JPEG_LABEL = ".tif", ".jpeg"
    new_names_list = list(df["IDENTIFIER"].unique())
    df["PATHS JPEG"] = ""
    df["PATHS TIF"] = ""
    for i in range(len(filesname)):
        if filesname[i][-4:] == TIF_LABEL:
            tif_new_name = rename_single_postcard(TIF_LABEL, dirpath, filesname, i, new_names_list)
            # save the new tif image name in the PATHS TIF column
            df.loc[df.index[i // 2], "PATHS TIF"] = tif_new_name + ", " + df.at[i // 2, "PATHS TIF"]

            # create a copy in JPEG format in the F_JPG folder
            im = Image.open(dirpath + "\\" + tif_new_name)
            im.save(dirpath[:-5] + "F_JPG\\" + tif_new_name[:-4] + ".jpeg", "JPEG")

            # save the new jpeg image name in the PATHS JPG column
            df.loc[df.index[i // 2], "PATHS JPEG"] = tif_new_name[:-4] + ".jpeg, " + df.at[i // 2, "PATHS JPEG"]
    return df


def rename_single_postcard(label, dirpath, filesname, i, new_names):
    """
    Helper function for rename_by_identifier that rename each image in folder.
    """
    FRONT_LABEL = "F"
    BACK_LABEL = "B"
    result_name = ""
    df_modified_index = (i - 1) // 2  # new names(uniques per postcard)
    if BACK_LABEL == filesname[i][-5]:
        result_name = new_names[df_modified_index] + "." + BACK_LABEL + label
    elif FRONT_LABEL == filesname[i][-5]:
        result_name = new_names[df_modified_index] + "." + FRONT_LABEL + label
    new_name = dirpath + "\\" + result_name
    old_name = dirpath + "\\" + filesname[i]
    os.rename(old_name, new_name)
    return result_name


######################################################################################################################
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~IMPLEMENT AT WORK~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
######################################################################################################################


# Folder Structure:
# DataBase Folder > Publisher > Divider > Excel table, Jpeg Images Folder > images.jpeg, TIF Images Folder > images.tif

# Pseudo Code:
# For every Publisher Folder in DataBase Folder:
#     For every Divider in Publisher Folder:
#         1) Find Excel Table
#         2) PreProcess Excel Table:
#            a) replace columns name " " with "_"
#            b) make SERIAL columns to map multiple postcard with same POSTCARD_NUM (use to make Identifier column)
#            c) make new column name Identifier by template P[0-9]{5}\.D[0-9]{3}\.PN[0-9]{5}\.S[0-9]{3}
#         3) For every Image in TIF Images Folder:
#            a) rename tif image by Identifier column and image ends letter(P[0-9]{3}\.D[0-9]{3}\.PN[0-9]{5}\.S[0-9]{3})
#            b) save new tif name is excel PATHS TIF column
#            c)make a JPEG copy from the TIF image and store it in Jpeg Images Folder
#            d) save new jpeg name is excel PATHs JPG column


for dirpath, foldernames, filesname in os.walk(DATABASE_PATH):  # search in database folder
    # make sure excel file is closed if not len(filesname) == 2
    if len(filesname) == 1 and filesname[0].endswith(".DATA_TABLE.xlsx"):
        out_path = dirpath + "\\" + filesname[0]
        current_df_of_xl = preprocess_dataset(out_path)
    elif dirpath.endswith(".F_TIF") and len(filesname) > 0:
        current_df_of_xl = rename_by_identifier(dirpath, filesname, current_df_of_xl)
        current_df_of_xl.to_excel(out_path, index=False)
