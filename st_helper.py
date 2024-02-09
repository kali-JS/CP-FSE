from base64 import b64encode
 
def get_base64(bin_file):
    with open(bin_file, 'rb') as f:
        data = f.read()
    return b64encode(data).decode()

def set_background(png_file,st):
    bin_str = get_base64(png_file)
    page_bg_img = '''
    <style>
    .stApp {
    background-image: url("data:image/png;base64,'''+bin_str+'''");
    background-size: 100% 100%;
    background-repeat: no-repeat;
    background-attachment: scroll; # doesn't work
    }
    </style>
    '''
    st.markdown(page_bg_img, unsafe_allow_html=True)
    return


def remove_menu(st):
    hide_streamlit_style = """
                <style>
                #MainMenu {visibility: hidden;}
                </style>
                """
    st.markdown(hide_streamlit_style, unsafe_allow_html=True)

def remove_footer(st):
    hide_streamlit_style = """
                <style>
                footer {visibility: hidden;}
                </style>
                """
    st.markdown(hide_streamlit_style, unsafe_allow_html=True)

def set_sidebar_width(st,value):
    set_sidebar_width =  """
        <style>
        [data-testid="stSidebar"][aria-expanded="true"] > div:first-child {
            width:""" + str(value) + """px;
        }
        [data-testid="stSidebar"][aria-expanded="false"] > div:first-child {
            width:""" + str(value) + """px;
            margin-left: -500px;
        }
        </style>
        """
    st.markdown(set_sidebar_width, unsafe_allow_html=True)

def set_layout_margin(st, valor):
    st.markdown(
            f"""
    <style>
        .reportview-container .main .block-container{{
            max-width: """ + str(valor) +"""px;
            padding-top:    0;
            padding-bottom: 0;
            margin-top:  0;
            margin-bottom:  0;
        }}
        .reportview-container .main {{
            color: {COLOR};
            background-color: {BACKGROUND_COLOR};
        }}
    </style>
    """,
            unsafe_allow_html=True,
    )
def hide_image_fullscreen_icon(st):
    hide_img_fs = '''
    <style>
    button[title="View fullscreen"]{
        visibility: hidden;}
    </style>
    '''
    st.markdown(hide_img_fs, unsafe_allow_html=True)