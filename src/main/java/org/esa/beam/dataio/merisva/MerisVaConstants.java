/*
 * Copyright (C) 2002 by Brockmann Consult (info@brockmann-consult.de)
 *
 * This program is free software; you can redistribute it and/or modify it
 * under the terms of the GNU General Public License as published by the
 * Free Software Foundation. This program is distributed in the hope it will
 * be useful, but WITHOUT ANY WARRANTY; without even the implied warranty
 * of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
 * See the GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 59 Temple Place - Suite 330, Boston, MA  02111-1307, USA.
 */

package org.esa.beam.dataio.merisva;

public class MerisVaConstants {
    public static final String PRODUCT_NAME_ATT_NAME = "ProductName";
    public static final String PRODUCT_TYPE_ATT_NAME = "ProductType";
    public static final String SCENE_WIDTH_ATT_NAME = "SceneColumnCount";
    public static final String SCENE_HEIGHT_ATT_NAME = "SceneLineCount";
    public static final String TIE_PT_COL_CNT_ATT_NAME = "TiePointColumnCount";
    public static final String TIE_PT_LINE_CNT_ATT_NAME = "TiePointLineCount";
    public static final String TIE_PT_SUBS_ATT_NAME = "TiePointSubSampling";
    public static final String DESCRIPTION_ATT_NAME = "Description";
    public static final String UNIT_ATT_NAME = "Unit";
    public static final String DS_CLASS_ATT_NAME = "DSClass";
    public static final String FLAGS_DS_ATT_NAME = "FlagsDS";
    public static final String FLAG_CODE_PATTERN = "QualityFlags.bit";
    public static final String[] FLAG_SEPARATION_STRINGS = {"\t- ", " - "};

    public static final String ROOT_GROUP_NAME = "/";
    public static final String SCENE_GROUP_NAME = "/SceneData";
    public static final String TIE_POINT_GROUP_NAME = "/TiePointData";

    public static final String LAT_TIE_POINT_NAME = "LAT";
    public static final String LON_TIE_POINT_NAME = "LON";

    public static final String SCALE_ATT_NAME = "scale";
    public static final String OFFSET_ATT_NAME = "offset";
}
