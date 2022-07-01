def transform_data():
    """Transforme los archivos xls a csv.

    Transforme los archivos data_lake/landing/*.xls a data_lake/raw/*.csv. Hay
    un archivo CSV por cada archivo XLS en la capa landing. Cada archivo CSV
    tiene como columnas la fecha en formato YYYY-MM-DD y las horas H00, ...,
    H23.

    """
    import pandas as pd

    
    encabezado = ['Fecha', 'H00', 'H01', 'H02', 'H03', 'H04', 'H05', 'H06', 'H07', 'H08', 'H09', 'H10', 'H11', 'H12', 'H13', 'H14', 'H15', 'H16', 'H17', 'H18', 'H19', 'H20', 'H21', 'H22', 'H23']
    
    
    for num in range(1995, 2022):        
        if num in range(1995, 2000):
            data_csv = pd.read_excel('data_lake/landing/{}.{}'.format(num, 'xlsx'), header=3)
            data_csv = data_csv.iloc[:, 0:25]
            data_csv.columns = encabezado       
            data_csv.to_csv('data_lake/raw/{}.csv'.format(num),index=None)
        elif(num in range(2000, 2016)):
            data_csv = pd.read_excel('data_lake/landing/{}.{}'.format(num, 'xlsx'), header=2)
            data_csv = data_csv.iloc[:, 0:25]
            data_csv.columns = encabezado             
            data_csv.to_csv('data_lake/raw/{}.csv'.format(num),index=None)
        elif(num in range(2016, 2018)):
            data_csv = pd.read_excel('data_lake/landing/{}.{}'.format(num, 'xls'), header=2)
            data_csv = data_csv.iloc[:, 0:25]
            data_csv.columns = encabezado             
            data_csv.to_csv('data_lake/raw/{}.csv'.format(num), index=None)
        else:
            data_csv = pd.read_excel('data_lake/landing/{}.{}'.format(num, 'xlsx'), header=0)
            data_csv = data_csv.iloc[:, 0:25]
            data_csv.columns = encabezado            
            data_csv.to_csv('data_lake/raw/{}.csv'.format(num), index=None)


    #raise NotImplementedError("Implementar esta función")


if __name__ == "__main__":
    import doctest

    doctest.testmod()
