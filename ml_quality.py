import pandas_profiling as pp
from ydata_profiling import ProfileReport
import json
import numpy as np
import tkinter as tk
from tkinter import ttk
from tkinter import filedialog as fd
import pandas as pd
import pyodbc
import json
import sys
from tkinter import messagebox
import cx_Oracle as cx
import os
import psycopg2
import warnings
from datetime import datetime
warnings.filterwarnings("ignore")





conexion = psycopg2.connect(
    host = "localhost",
    port = "5432",
    user = "postgres",
    password = "123456789",
    
)

#Métodos#

def perfil(filename , perfilID):
    try:
        
        with open('{0}.json'.format(filename), "r") as file:
                json_file= json.load(file)

        data = pd.DataFrame(json_file["variables"]).T
        data["ID_PERFILADO"] = perfilID*data.shape[0]
        data["FECHA"] = datetime.now()

        #Sacamos las estadísticas que nos interesan para nuestro perfilado 
        needs = ['ID_PERFILADO','FECHA', 'p_distinct', 'p_unique', 'type','p_missing', 'value_counts_without_nan', 'max_length', 'mean_length',
            'min_length', 'p_negative', 'mean', 'std', 'min', 'max', '25%', '50%', '75%']

        data= data[[i for i in data.columns if i in needs]]

        columnas_perfilado = ["ID_PERFILADO","NAME_COLUMN", "N_COLUMNAS", "N_FILAS", "FECHA", "TIPO_DATO", "P_DISTINTOS","P_UNICOS", "P_FALTANTES", "MUESTRA",
                            'MAXIMO', 'MEDIA', 'MINIMO','MAX_LENGTH', 'MIN_LENGTH', 'P_NEGATIVOS', 'STD', 'PC_25', 'MEDIANA', 'PC_75']
        df_perfilado = pd.DataFrame(columns= columnas_perfilado)

        # Insertamos los datos en nuestra tabla original de perfilado

        df_perfilado["ID_PERFILADO"]= data["ID_PERFILADO"]
        df_perfilado["N_COLUMNAS"]= json_file["table"]["n_var"]
        df_perfilado["P_DISTINTOS"]= data["p_distinct"]
        df_perfilado["N_FILAS"]= json_file["table"]["n"]
        df_perfilado["FECHA"]= data["FECHA"]
        df_perfilado["TIPO_DATO"]= data["type"]
        df_perfilado["P_FALTANTES"]= data["p_missing"]
        df_perfilado["P_UNICOS"] = data["p_unique"]
        df_perfilado["MUESTRA"]= data["value_counts_without_nan"]
        df_perfilado["NAME_COLUMN"] = df_perfilado.index

        

        for v, k in data.iterrows():
            if k["type"] == "Text":
                df_perfilado['MAX_LENGTH'] = data["max_length"]
                df_perfilado['MIN_LENGTH']= data["min_length"]
                
            else:
                df_perfilado["P_NEGATIVOS"]= data["p_negative"]
                df_perfilado['STD'] = data["std"]
                df_perfilado['PC_25'] = data["25%"]
                df_perfilado['MEDIANA']= data["50%"]
                df_perfilado['PC_75']= data["75%"]
                df_perfilado['MAXIMO'] = data["max"]
                df_perfilado['MINIMO']= data["min"]
                df_perfilado['MEDIA']= data["mean"]

        # Generamos un nuevo dataframe para el DRILL DOWN de las columnas
        
        columna_muestra = ["RES_CALCULO", "DDWN_PATTERN_FREQ", "DDWN_PATTERN_LEN", "DDWN_PATTERN_PERC", 
                           "ID_PERFILADO", "NAME_COLUMN"]
        ddwn = pd.DataFrame(columns=columna_muestra)

        res, freq, length, perc, names = [], [], [], [], []
        N = df_perfilado["N_FILAS"][0] #Extraemos el numero de muestras de la tabla para calcular el porcentaje 
        for name, d in df_perfilado["MUESTRA"].items():
            for k,v in d.items(): 
                res.append(k)
                freq.append(v)
                length.append(len(k))
                perc.append((v/N)*100)
                names.append(name)

        ddwn["RES_CALCULO"] = res
        ddwn["DDWN_PATTERN_FREQ"] = freq
        ddwn["DDWN_PATTERN_LEN"] = length
        ddwn["DDWN_PATTERN_PERC"] = perc
        ddwn["ID_PERFILADO"] = perfilID*ddwn.shape[0]
        ddwn["NAME_COLUMN"] = names

        
        return df_perfilado, ddwn
    except BaseException as e:
            print(str(e))
            sys.exit()

def mostrar_instrucciones():
    messagebox.showinfo("Instrucciones", "1. Elige el nombre del fichero perfilado"+"\n"+"2. Selecciona el origen de los datos" +"\n"+ "3. Haz clic en el botón 'Data Profiling' para iniciar el perfilado.")

def fin():
    sys.exit()

def conectar_bbdd():
    global server
    global user
    global password
    global dbname
    global query_global
    server = tk.StringVar()
    server = server_entry.get()
    user = tk.StringVar()
    user = user_entry.get()
    password = tk.StringVar()
    password = pwd_entry.get()  
    dbname = tk.StringVar()
    dbname = dbname_entry.get()  
    query_global = tk.StringVar()
    query_global = query_entry.get()  
    ventana_conexion.destroy()
    ventana.destroy()
    
    

def read_ticks(): 
    global list_rules
    global variables

    ventana2.destroy()
    result = [ ing for ing, cb in zip( list_rules, variables ) if cb.get()>0 ] 

    # A partir de list rule extraure la tipologia de la regla i el nom i aplicar la validació en questió
    # Exemple: UNIQUENESS_RULE_DNI --> Tipologia UNIQUENESS (invocar método uniqueness) i Camp a validar DNI
    
    if len(result) > 0:
        pk = (result[0]).split("-")[2]
        detalleerror = pd.DataFrame()
        for list in result:
            regla = list.split("-")[0]
            field = list.split("-")[2]
            result_errores = validateDataframe(dataframe,field,regla)
            if result_errores.size != 0:
                        d = { "PK_VALUE": dataframe[result_errores == False]["{}".format(pk)], 'FIELD': field ,'RULE': regla ,'ID_PROFILE': id_profile }
                        dat_error = pd.DataFrame(d)
                        detalleerror = detalleerror.append(dat_error)
            else:
                pass
        print(detalleerror)
        
        
        
        values = [(row.PK_VALUE, row.FIELD, row.RULE,row.ID_PROFILE) for index, row in detalleerror.iterrows()]

        # Ejecuta la sentencia SQL con executemany()
        cursor.executemany('INSERT INTO errordetails ("PK_VALUE","FIELD","RULE" , "ID_PROFILE") values (%s, %s,%s, %s)', values)

        # Aplica los cambios a la base de datos
        conexion.commit()
        
        summary = tk.Tk()
        summary.title("Summary")
        summary.config(width=1000, height=1000)

        num_registros_nok = detalleerror.groupby(['FIELD', 'RULE']).size().reset_index(name='NUM_ROWS_NOK')
        num_registros_nok.insert(2,'NUM_ROWS_TOTAL', dataframe.shape[0])
        
        print(num_registros_nok)
        # Crea una lista de tuplas con los valores a insertar
        

        for index, row in num_registros_nok.iterrows():
            cursor.execute('''INSERT INTO agdvalidations ("ID_PROFILE","RULE","FIELD" , "NUM_ROWS_TOTAL" , "NUM_ROWS_NOK") values ('{0}' , '{1}' , '{2}' , '{3}' , '{4}')'''.format(id_profile,row.RULE,row.FIELD,row.NUM_ROWS_TOTAL,row.NUM_ROWS_NOK))
        conexion.commit()
        text = tk.Text(summary)
        text.config(width=500, height=500)
        for i in range(len(num_registros_nok)):
            text.insert("end", "\n" )
            text.insert(tk.INSERT,"FIELD: " + str(num_registros_nok.iloc[i,0]))
            text.insert(tk.INSERT,"  ")
            text.insert(tk.INSERT,"RULE: " + str(num_registros_nok.iloc[i,1]))
            text.insert(tk.INSERT,"  ")
            text.insert(tk.INSERT,"NUM_ROWS_TOTAL: "  + str(num_registros_nok.iloc[i,2]))
            text.insert(tk.INSERT,"  ")
            text.insert(tk.INSERT,"NUM_ROWS_NOK: " + str(num_registros_nok.iloc[i,3]))

        text.pack()
        summary.mainloop()
        print("Reglas de Calidad Aplicadas")

    else:
        print('No se han seleccionado reglas de calidad')

def COMPLETENESSValidation(dataframe, field):


    result = pd.DataFrame(~dataframe[field].isnull())
    # Se obtiene la última columna como una serie
    result = result.iloc[:, -1]
    return result
        

def UNIQUENESSValidation(dataframe, field):
    

    result = pd.DataFrame(~dataframe[field].duplicated(keep=False))
    # Se obtiene la última columna como una serie
    result = result.iloc[:, -1]
    return result
    

def validateDataframe(dataframe,field,rule):
        if rule == 'UNIQUENESS':
            return UNIQUENESSValidation(dataframe, field)

        elif rule == 'COMPLETENESS':
            return COMPLETENESSValidation(dataframe, field)

        else:
            pass
def perfilar():
    global variable
    global nombrearch
    global dataframe
    global user_entry
    global pwd_entry
    global server_entry
    global dbname_entry
    global query_entry
    global ventana
    global ventana_conexion
    global filename

    filename = tk.StringVar()
    filename = nombrearchivo_entry.get()
    
    variable = combo.get()
    if "EXCEL" in str(variable) or "CSV" in str(variable)  or "PARQUET" in str(variable):
        try:
            nombrearch=fd.askopenfilename(initialdir = "/",title = "Seleccione archivo",filetypes=(("Excel files", ".xlsx .xls .csv"),("todos los archivos","*.*")))
            if nombrearch=='':
                print('Error de archivo')
                sys.exit()
            elif  'xlsx' in nombrearch:
                dataframe = pd.read_excel(nombrearch )
            elif 'parquet' in nombrearch:
                dataframe = pd.read_parquet(nombrearch)
            elif 'csv' in nombrearch:
                dataframe = pd.read_csv(nombrearch )
            elif 'json' in nombrearch:
                dataframe = pd.read_json(nombrearch )
                
            else:
                pass
        except Exception as e:
            # Atrapar error
            print('Archivo Erroneo ' + str(e))
            sys.exit() 
        
        ventana.destroy()
    else:
        ventana_conexion = tk.Tk()
        ventana_conexion.config(width=500, height=500)
        user_label = tk.Label(ventana_conexion, text='USUARIO' , padx = 5 , pady = 5)
        user_entry = tk.Entry(ventana_conexion)
        pwd_label = tk.Label(ventana_conexion, text='PASSWORD', padx = 5 , pady = 5)
        pwd_entry = tk.Entry(ventana_conexion, show = '*' )
        server_label = tk.Label(ventana_conexion, text='SERVER', padx = 5 , pady = 5)
        server_entry = tk.Entry(ventana_conexion )
        db_name_label = tk.Label(ventana_conexion, text='DATABASE_NAME', padx = 5 , pady = 5)
        dbname_entry = tk.Entry(ventana_conexion )
        query_label = tk.Label(ventana_conexion, text='QUERY TABLA A PERFILAR', padx = 5 , pady = 5)
        query_entry = tk.Entry(ventana_conexion )

        user_label.pack()
        user_entry.pack()
        pwd_label.pack()
        pwd_entry.pack()
        server_label.pack()
        server_entry.pack()
        db_name_label.pack()
        dbname_entry.pack()
        query_label.pack()
        query_entry.pack()

        button_conectar = tk.Button(ventana_conexion, text = "Conectar BBDD", command=conectar_bbdd)
        button_conectar.pack()
        ventana_conexion.mainloop()

        if 'SQL_SERVER' in variable:
            try:
                cnxn = pyodbc.connect('DRIVER={ODBC Driver 17 for SQL Server};SERVER='+server+';DATABASE='+dbname+';UID='+user+';PWD='+ password)
            except Exception as e:
                # Atrapar error
                print('Conexión fallida ' + str(e))
                sys.exit()
        elif 'ORACLE' in variable:
            try:
                cnxn = pyodbc.connect('DRIVER={Devart ODBC Driver for Oracle};Direct=True;Host={0};Service Name={1};User ID={2};Password={3}'.format(server,dbname,user,password))
                
            except Exception as e:
                # Atrapar error
                print('Conexión fallida ' + str(e))
                sys.exit()
        elif 'POSTGRES' in variable:
            try:
                cnxn = psycopg2.connect(host = server,port = dbname,user = user,password = password)
                
            except Exception as e:
                # Atrapar error
                print('Conexión fallida ' + str(e))
                sys.exit()
        else:
            try:
                cnxn = pyodbc.connect("DRIVER={MySQL ODBC 8.0 ANSI Driver}; SERVER={0}; UID={1}; PASSWORD={2};".format(server,user,password))
                
            except Exception as e:
                # Atrapar error
                print('Conexión fallida ' + str(e))
                sys.exit()
            
        query = query_global
        dataframe = pd.read_sql_query(query, con=cnxn)
        
    

ventana = tk.Tk()
ventana.title("Data Source")
ventana.config(width=500, height=500)
nombrearchivo_label = tk.Label(ventana, text='NOMBRE ARCHIVO' + str('\n') + 'PERFILADO')
nombrearchivo_entry = tk.Entry(ventana)
nombrearchivo_label.place(x=100,y=60)
nombrearchivo_entry.place(x=220,y=60)
etiqueta_data_source = ttk.Label(text="DATA SOURCE: ")
etiqueta_data_source.place(x=100, y=100)
ventana.config(bg="#ADD8E6")
ventana.after(0, mostrar_instrucciones)
boton_perfilado = tk.Button(text="Data Profiling", bg="#FFFFFF", command=perfilar)
boton_perfilado.place(x=190, y=190)
combo = ttk.Combobox(
    state="readonly",
    values=["", "EXCEL" , "PARQUET" , "CSV" , "BBDD_AZURE_SQL_SERVER" , "BBDD_ORACLE" , "BBDD_MYSQL" , "BBDD_POSTGRESQL"],
    width = 25

)
combo.place(x=220, y=100)
ventana.mainloop()



list_rules = []
print(dataframe.info())
for i,j in zip(dataframe.columns, dataframe.dtypes):
    
    if ("int64"in str(j)):
        dataframe['{}'.format(i)]=dataframe['{}'.format(i)].astype('int16')
    else:
        pass
    
    if ("float64"in str(j)):
        dataframe['{}'.format(i)]=dataframe['{}'.format(i)].astype('float16')
    else:
        pass


print(dataframe.info())
profile = ProfileReport(dataframe, title="Pandas Profiling Report" , minimal =True)
profile.to_file('{0}_PROFILING.html'.format(filename))
profile.to_file('{0}_PROFILING.json'.format(filename))

# Obtención del ID_PROFILE y inserción en la BBDD de la URL para poder consultar n veces el fichero html.
cursor = conexion.cursor()
cursor.execute('SELECT MAX("ID_PERFILADO")  FROM "DQ_PROFILING"')
resultados = pd.DataFrame()
resultados['MAXIMUM'] = pd.DataFrame(cursor.fetchall())
id_profile = sum(filter(None,[resultados['MAXIMUM'].iloc[0],1]))




ruta_absoluta = os.path.abspath('{0}_PROFILING.html'.format(filename))


dperfilado,ddown=perfil('{0}_PROFILING'.format(filename), id_profile)
print(dperfilado)
print(ddown)


dperfilado['URL'] = ruta_absoluta
for index, row in dperfilado.iterrows():
    cursor.execute('''INSERT INTO \"DQ_PROFILING\" ("ID_PERFILADO","N_COLUMNAS" , "NAME_COLUMN" , "N_FILAS" , "FECHA" , "TIPO_DATO" , "P_UNICOS" , "P_FALTANTES" , "MAX_LENGTH" , "MIN_LENGTH" , "MAXIMO" , "MINIMO" , "MEDIA" , "P_NEGATIVOS" , "STD" , "PC_25" , "MEDIANA" , "PC_75","URL") values ('{0}' , '{1}' , '{2}' , '{3}' , '{4}' , '{5}' , '{6}' , '{7}' , '{8}' , '{9}' , '{10}' , '{11}' , '{12}' , '{13}' , '{14}' , '{15}' , '{16}' , '{17}' , '{18}' )'''.format(row.ID_PERFILADO,row.N_COLUMNAS,row.NAME_COLUMN , row.N_FILAS , row.FECHA , row.TIPO_DATO , row.P_UNICOS , row.P_FALTANTES , row.MAX_LENGTH , row.MIN_LENGTH , row.MAXIMO , row.MINIMO , row.MEDIA , row.P_NEGATIVOS , row.STD , row.PC_25 , row.MEDIANA , row.PC_75 , row.URL) )
    conexion.commit()

for index, row in ddown.iterrows():
    cursor.execute('''INSERT INTO \"DQ_DDWN_PROFILING\" ("ID_PERFILADO","NAME_COLUMN" , "RES_CALCULO" , "DDWN_PATTERN_FREQ" , "DDWN_PATTERN_LEN" , "DDWN_PATTERN_PERC") values ('{0}' , '{1}' , '{2}' , '{3}' , '{4}' , '{5}')'''.format(row.ID_PERFILADO,row.NAME_COLUMN , row.RES_CALCULO , row.DDWN_PATTERN_FREQ , row.DDWN_PATTERN_LEN , row.DDWN_PATTERN_PERC  ) )
    conexion.commit()

ventana2 = tk.Tk()
ventana2.title("Data Quality Rules")
ventana2.config(width=500, height=500)


with open('{0}_PROFILING.json'.format(filename)) as archivo:

    datos = json.load(archivo)

    for column,values in datos['variables'].items():

        if values['is_unique'] == False :
            rule = 'UNIQUENESS-RULE-{0}'.format(column)
            list_rules.append(rule)
        else:
            pass

        if values['n_missing'] != 0:
            rule = 'COMPLETENESS-RULE-{0}'.format(column)
            list_rules.append(rule)
        else:
            pass

            





txt = tk.Text(ventana2, width=40, height=20) 
variables = [] 
for i in list_rules: 
    variables.append( tk.IntVar(value=0) ) 
    cb = tk.Checkbutton( txt, text = i, variable = variables[-1] ) 
    txt.window_create( "end", window=cb ) 
    txt.insert( "end", "\n" )
    
txt.pack() 

but = tk.Button(ventana2, text = 'Iniciar DQ Rules', command = read_ticks) 
but.pack()  
but2 = tk.Button(ventana2, text = 'Cerrar Herramienta', command=fin) 
but2.pack() 

ventana2.mainloop()


