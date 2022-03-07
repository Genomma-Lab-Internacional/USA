import pandas as pd
import pyodbc

def main(start_date, final_date, dtiempo, dclientes, dproductos):
    
    conn_dwh = pyodbc.connect('Driver={SQL Server};'
                              'Server=SFEDWH01;'
                              'Database=Gnm_DWH;'
                              'Trusted_Connection=yes;')
    
    # Import data from AWS
    query = '''SELECT TmpID, ProPstID, SucID, SUM(SoutCantDesp) SoutCantDesp, SUM(SoutCantExist) SoutCantExist, SUM(SoutMontoDesp) SoutMontoDesp, SUM(SoutMontoExist) SoutMontoExist
               FROM Gnm_DWH.dbo.Fact_Desplazamiento_SemanalUSA WHERE TmpID BETWEEN {0} AND {1} AND (SoutCantDesp <> 0 OR SoutCantExist <> 0)  AND SucID NOT IN (139013) AND ProPstID <> 7885
               GROUP BY TmpID, ProPstID, SucID
               ORDER BY TmpID, ProPstID, SucID;'''
               
    data = pd.read_sql(query.format(start_date, final_date), conn_dwh)

    # Clean both columns
    data['TmpID'] = data['TmpID'].astype('int64')
    data['TmpID'] = data['TmpID'].map(str)
    dtiempo['TmpID'] = dtiempo['TmpID'].astype('int64')
    dtiempo['TmpID'] = dtiempo['TmpID'].map(str)
    
    # Merge AWS data (so) and time data (ti)
    soti = pd.merge(data, dtiempo, on='TmpID', how='left')

    # Clean both columns
    dclientes.rename({'SucId':'SucID'}, axis=1, inplace=True)
    dclientes['SucID'] = dclientes['SucID'].astype('int64')
    dclientes['SucID'] = dclientes['SucID'].map(str)
    soti['SucID'] = soti['SucID'].astype('int64')
    soti['SucID'] = soti['SucID'].map(str)
    
    # Merge soti data and clients data (c)
    sotic = pd.merge(soti, dclientes, on='SucID', how='left')

    # Clean both columns
    dproductos['ProPstID'] = dproductos['ProPstID'].map(str)
    sotic['ProPstID'] = sotic['ProPstID'].astype('int64')
    sotic['ProPstID'] = sotic['ProPstID'].map(str)
    
    # Merge sotic data and skus data
    total = pd.merge(sotic, dproductos, on='ProPstID', how='left')

    desconv = total.pivot_table(index=['TmpAnioSemanaGenomma', 'TmpSemanaAnioGenomma', 'TmpMesGenomma', 'TmpFecha', 'GrpID', 'GrpNombre',
                                       'CadID', 'CadNombre', 'MrcNombre', 'ProID', 'ProNombre', 'ProPstID', 'ProPstCodBarras', 'ProPstNombre'],
                                values=['SoutCantDesp', 'SoutCantExist', 'SoutMontoDesp', 'SoutMontoExist'],
                                 aggfunc='sum').reset_index()

    desconv.reset_index(drop=True, inplace=True)

    desconv.rename({'TmpFecha':'Fecha', 'TmpAnioSemanaGenomma':'Anio GL', 'TmpSemanaAnioGenomma':'Semana GL', 
                    'TmpMesGenomma':'Mes GL', 'SoutCantDesp':'Unidades Ventas', 'SoutCantExist':'Unidades Inventario',
                   'SoutMontoDesp':'Monto Ventas', 'SoutMontoExist':'Monto Inventario'}, axis=1, inplace=True)
    
    return desconv