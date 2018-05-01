import java.sql.*;

import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.LinkedList;
import java.io.BufferedReader;
import java.io.FileReader;
import java.util.List;

@SuppressWarnings("SqlNoDataSourceInspection")
public class Diagnostico {

    private final String DATAFILE = "data/disease_data.data";
    private Connection mConnection;     //Variable global que contiene la conexión a la BD

    private final String SERVER = "localhost:3306";     //Dirección del servidor
    private final String USER = "bddx";                 //Usuario de la conexión
    private final String PASS = "bddx_pwd";             //Contraseña de la conexión
    private final String NOMBRE_BASE_DE_DATOS = "diagnostico";  //Nombre de la base de datos
    private final String CABECERA_CONEXION = "jdbc:mysql://";   //Cabecera de la conexion
    private final String DRIVER = "com.mysql.jdbc.Driver";

    /* Nombres de las tablas de la base de datos */
    private final String TABLE_DISEASE = "disease";
    private final String TABLE_DISEASE_SYMPTON = "disease_sympton";
    private final String TABLE_SYMPTON = "sympton";
    private final String TABLE_SYMPTON_SEMANTIC_TYPE = "sympton_semantic_type";
    private final String TABLE_SEMANTIC_TYPE = "semantic_type";
    private final String TABLE_DISEASE_HAS_CODE = "disease_has_code";
    private final String TABLE_CODE = "code";
    private final String TABLE_SOURCE = "source";

    /* Columnas de disease */
    private final String DISEASE_ID = "disease_id";
    private final String DISEASE_NAME = "name";
    /*Columnas de sympton */
    private final String SYMPTON_CUI = "cui";
    private final String SYMPTON_NAME = "name";
    /*Columnas de semantic type*/
    private final String SEMANTYC_TYPE_ID = "semantic_type_id";
    private final String SEMANTYC_TYPE_CUI = "cui";
    /*Columnas de code*/
    private final String CODE_ID = "code";
    /*Columnas de source*/
    private final String SOURCE_ID = "source_id";
    private final String SOURCE_NAME = "name";


    private final String TEST_URL = "192.168.146.131";

    private void showMenu() {

        int option = -1;
        do {
            System.out.println("Bienvenido a sistema de diagnóstico\n");
            System.out.println("Selecciona una opción:\n");
            System.out.println("\t1. Creación de base de datos y carga de datos.");
            System.out.println("\t2. Realizar diagnóstico.");
            System.out.println("\t3. Listar síntomas de una enfermedad.");
            System.out.println("\t4. Listar enfermedades y sus códigos asociados.");
            System.out.println("\t5. Listar síntomas existentes en la BD y su tipo semántico.");
            System.out.println("\t6. Mostrar estadísticas de la base de datos.");
            System.out.println("\t7. Salir.");
            try {
                option = readInt();
                switch (option) {
                    case 1:
                        crearBD();
                        break;
                    case 2:
                        realizarDiagnostico();
                        break;
                    case 3:
                        listarSintomasEnfermedad();
                        break;
                    case 4:
                        listarEnfermedadesYCodigosAsociados();
                        break;
                    case 5:
                        listarSintomasYTiposSemanticos();
                        break;
                    case 6:
                        mostrarEstadisticasBD();
                        break;
                    case 7:
                        exit();
                        break;
                }
            } catch (Exception e) {
                System.err.println("Opción introducida no válida!");
            }
        } while (option != 7);
        exit();
    }

    private void exit() {
        System.out.println("Saliendo.. ¡hasta otra!");
        if (mConnection != null) {
            try {
                mConnection.close();    //Cerrar la conexión
                mConnection = null;     //Liberar el recurso
                System.out.println("Conexion cerrada");
            } catch (SQLException e) {
                System.err.println("Error cerrando la conexión");
            }
        }
        System.exit(0);
    }

    private void conectar() {
        try {
            Class.forName(DRIVER);
        } catch (ClassNotFoundException e) {
            System.err.println("No se ha cargado el Driver por favor cargalo");
            return;
        }
        String url = CABECERA_CONEXION + TEST_URL + "/";   //Crear la URL de la conexión
        try {
            mConnection = DriverManager.getConnection(url, USER, PASS);     //Iniciar la conexion
        } catch (SQLException e) {
            System.err.println("Error al iniciar la conexión");
        }
        try {
            mConnection.setCatalog(NOMBRE_BASE_DE_DATOS);                   //Elegir la base de datos
        } catch (SQLException e) {
            System.err.println("No se pudo seleccionar la base de datos " + NOMBRE_BASE_DE_DATOS +
                    " por favor creela");
        }

        System.out.println("Conexion realizada correctamente!");
    }

    private void crearBD() {
        if (checkIfDatabaseExists(NOMBRE_BASE_DE_DATOS)) {
            menuBorrarBaseDeDatos();
        } else {
            crearBase();
        }
    }

    private void realizarDiagnostico() {
        // implementar
    }

    private void listarSintomasEnfermedad() {
        // implementar
    }

    private void listarEnfermedadesYCodigosAsociados() {
        // implementar
    }

    private void listarSintomasYTiposSemanticos() {
        // implementar
    }

    private void mostrarEstadisticasBD() {
        // implementar
    }

    /**
     * Método para leer números enteros de teclado.
     *
     * @return Devuelve el número leído.
     * @throws Exception Puede lanzar excepción.
     */
    private int readInt() throws Exception {
        try {
            System.out.print("> ");
            return Integer.parseInt(new BufferedReader(new InputStreamReader(System.in)).readLine());
        } catch (Exception e) {
            throw new Exception("Not number");
        }
    }

    /**
     * Método para leer cadenas de teclado.
     *
     * @return Devuelve la cadena leída.
     * @throws Exception Puede lanzar excepción.
     */
    private String readString() throws Exception {
        try {
            System.out.print("> ");
            return new BufferedReader(new InputStreamReader(System.in)).readLine();
        } catch (Exception e) {
            throw new Exception("Error reading line");
        }
    }

    /**
     * Método para leer el fichero que contiene los datos.
     *
     * @return Devuelve una lista de String con el contenido.
     * @throws Exception Puede lanzar excepción.
     */
    private LinkedList<String> readData() throws Exception {
        LinkedList<String> data = new LinkedList<String>();
        BufferedReader bL = new BufferedReader(new FileReader(DATAFILE));
        while (bL.ready()) {
            data.add(bL.readLine());
        }
        bL.close();
        return data;
    }

    /**
     * Comprueba si la base de datos existe en la conexión
     *
     * @param databaseName El nombre de la base de datos que se desea comprobar si existe
     * @return Devuelve cierto en caso de que la base de datos exista
     */
    private boolean checkIfDatabaseExists(String databaseName) {
        if (!checkIfIsConnected()) {
            conectar();     //Si no existe la conexión crearla
        }
        try {
            ResultSet resultSet = mConnection.getMetaData().getCatalogs();
            boolean encontrado = false;
            while (resultSet.next() && !encontrado) {
                encontrado = resultSet.getString(1).equals(databaseName);
            }
            resultSet.close();
            return encontrado;
        } catch (SQLException e) {
            System.err.println("Error obteniendo los metadatos");
            return false;
        }
    }

    /**
     * Comprueba si la conexión existe y si esta abierta
     *
     * @return Devuelve cierto en caso de que la conexión este correctamente establecida, falso en cualquier otro caso
     */
    private boolean checkIfIsConnected() {
        try {
            return mConnection != null && !mConnection.isClosed();
        } catch (SQLException e) {
            System.err.println("Error al comprobar el estado de la conexion");
            return false;
        }
    }

    /**
     * Menu para preguntar si borrar la BD
     */
    private void menuBorrarBaseDeDatos() {
        int option;
        System.out.println("La base de datos ya existe\n");
        System.out.println("¿Quiere eliminarla y crearla de nuevo?\n");
        System.out.println("\t1. Si.");
        System.out.println("\t2. No.");
        try {
            option = readInt();
            switch (option) {
                case 1:
                    borrarBD();
                    break;
                case 2:
                    break;
            }
        } catch (Exception e) {
            System.err.println("Opción introducida no válida!");
        }
    }

    /**
     * Realiza la eliminación de la base de datos
     */
    private void borrarBD() {
        try {
            Statement statement = mConnection.createStatement();    /*En este caso por dependencia de metodos por encima
             no es necesario comprobar la conexion*/
            statement.execute("DROP DATABASE " + NOMBRE_BASE_DE_DATOS);
            System.out.println("Base de datos eliminada correctamente");
            crearBase();
        } catch (SQLException e) {
            System.err.println("Error eliminando la base de datos");
        }
    }

    /**
     * Metodo el cual realiza la creacion de la base de datos
     */
    private void crearBase() {
        try {
            Statement statement = mConnection.createStatement();
            statement.execute("CREATE DATABASE " + NOMBRE_BASE_DE_DATOS);
            System.out.println("Base de datos creada correctamente");
            mConnection.setCatalog(NOMBRE_BASE_DE_DATOS); //Hacer que la conexión "apunte" a la base de datos
            crearTablas();
            insertarEnfermedadEnLaBD(parserDeDatos(readData()));
        } catch (SQLException e) {
            System.err.println("Error creando la base de datos");
        } catch (Exception e) {
            System.err.println("Error al leer el archivo de datos");
        }
    }

    /**
     * Creación de las tablas en la Base de datos
     */
    private void crearTablas() {
        try {
            Statement statement = mConnection.createStatement();

            statement.executeUpdate("CREATE TABLE " + TABLE_DISEASE + " (" + DISEASE_ID + " INT AUTO_INCREMENT, "
                    + DISEASE_NAME + " VARCHAR (255),PRIMARY KEY (" + DISEASE_ID + "));");


            statement.executeUpdate("CREATE TABLE " + TABLE_SYMPTON + "(" + SYMPTON_CUI + " VARCHAR(255), " + SYMPTON_NAME
                    + " VARCHAR(255), PRIMARY KEY (" + SYMPTON_CUI + "));");


            statement.executeUpdate("CREATE TABLE " + TABLE_SEMANTIC_TYPE + " (" + SEMANTYC_TYPE_ID + " INT AUTO_INCREMENT, " + SEMANTYC_TYPE_CUI
                    + " VARCHAR(45), PRIMARY KEY (" + SEMANTYC_TYPE_ID + "));");


            statement.executeUpdate("CREATE TABLE " + TABLE_SOURCE + " (" + SOURCE_ID + " INT AUTO_INCREMENT, " + SOURCE_NAME +
                    " VARCHAR(255) UNIQUE , PRIMARY KEY (" + SOURCE_ID + "));");


            statement.executeUpdate("CREATE TABLE " + TABLE_CODE + "(" + CODE_ID + " VARCHAR(255), " + SOURCE_ID +
                    " INT , PRIMARY KEY (" + CODE_ID + ")," + "" +
                    " FOREIGN KEY (" + SOURCE_ID + ")" + " REFERENCES " + TABLE_SOURCE + "(" + SOURCE_ID + "));");


            statement.executeUpdate("CREATE TABLE " + TABLE_DISEASE_HAS_CODE +
                    " (" + DISEASE_ID + " INT, " +
                    CODE_ID + " VARCHAR(255), " +
                    SOURCE_ID + " INT, " +
                    "PRIMARY KEY (" + DISEASE_ID + " ," + CODE_ID + " ," + SOURCE_ID + ")," +
                    "FOREIGN KEY (" + DISEASE_ID + " ) REFERENCES " + TABLE_DISEASE + "(" + DISEASE_ID +
                    "),FOREIGN KEY (" + CODE_ID + ") REFERENCES " + TABLE_CODE + "(" + CODE_ID +
                    "), FOREIGN KEY (" + SOURCE_ID + ") REFERENCES " + TABLE_SOURCE + "(" + SOURCE_ID + "));");


            statement.executeUpdate("CREATE TABLE " + TABLE_DISEASE_SYMPTON + " (" + DISEASE_ID + " INT, "
                    + SYMPTON_CUI + " VARCHAR(25),"
                    + "PRIMARY KEY " + "(" + DISEASE_ID + ", " + SYMPTON_CUI
                    + "), FOREIGN KEY (" + DISEASE_ID + ") REFERENCES " + TABLE_DISEASE + "(" + DISEASE_ID + "),"
                    + "FOREIGN KEY (" + SYMPTON_CUI + ") REFERENCES " + TABLE_SYMPTON + "(" + SYMPTON_CUI + "));");


            statement.executeUpdate("CREATE TABLE " + TABLE_SYMPTON_SEMANTIC_TYPE + "("
                    + SYMPTON_CUI + " VARCHAR(25), "
                    + SEMANTYC_TYPE_ID + " INT,"
                    + "PRIMARY KEY (" + SYMPTON_CUI + "," + SEMANTYC_TYPE_ID + "),"
                    + "FOREIGN KEY (" + SYMPTON_CUI + ") REFERENCES " + TABLE_SYMPTON + "(" + SYMPTON_CUI + "),"
                    + "FOREIGN KEY (" + SEMANTYC_TYPE_ID + ") REFERENCES " + TABLE_SEMANTIC_TYPE + "(" + SEMANTYC_TYPE_ID + "));");

            statement.close();


        } catch (SQLException e) {
            System.err.println("Error al crear las tablas");
        } finally {
            try {
                mConnection.setAutoCommit(true);
            } catch (SQLException e) {
                System.err.println("Error restableciendo el autocommit");
            }
        }
    }

    /**
     * Metodo que realiza el parse de datos del fichero pasado a Objetos Java
     *
     * @param strings Lista que contiene las lineas con la información de cada enfermedad
     * @return Lista enlazada de objetos enfermedad dentro de los cuales está toda la información
     */
    private LinkedList<Enfermedad> parserDeDatos(LinkedList<String> strings) {
        LinkedList<Enfermedad> enfermedades = new LinkedList<>();
        for (String linea : strings) {
            String[] resultados = linea.split(":", 2);
            String nombre = resultados[0];
            resultados = resultados[1].split("=");
            List<Codigo> codigos = new ArrayList<>();
            for (String codigo : resultados[0].split(";")) {
                String[] splited = codigo.split("@");
                codigos.add(new Codigo(splited[0], splited[1]));
            }
            List<Sintoma> sintomas = new ArrayList<>();
            for (String sintoma : resultados[1].split(";")) {
                String[] splitted = sintoma.split(":");
                sintomas.add(new Sintoma(splitted[0], splitted[1], splitted[2]));
            }
            enfermedades.addLast(new Enfermedad(nombre, codigos, sintomas));
        }
        return enfermedades;
    }

    private void insertarEnfermedadEnLaBD(LinkedList<Enfermedad> enfermedades) {
        try {
            mConnection.setAutoCommit(false);
            Statement statement = mConnection.createStatement();
            PreparedStatement preparedStatementDisease = mConnection.prepareStatement("INSERT INTO " + TABLE_DISEASE + " (" + DISEASE_NAME
                    + ") VALUES (?)");

            PreparedStatement preparedStatementDiseaseHasSympton = mConnection.prepareStatement("INSERT INTO " + TABLE_DISEASE_SYMPTON
                    + "(" + SYMPTON_CUI + "," + DISEASE_ID + ") VALUES (?,?)");
            PreparedStatement preparedStatementDiseaseHasCode = mConnection.prepareStatement("INSERT INTO " + TABLE_DISEASE_HAS_CODE + "("
                    + DISEASE_ID + "," + CODE_ID + "," + SOURCE_ID + ") VALUES (?,?,?)");
            for (Enfermedad enfermedad : enfermedades) {


                preparedStatementDisease.setString(1, enfermedad.nombre);
                preparedStatementDisease.executeUpdate();

                ResultSet resultSet = statement.executeQuery("SELECT " + DISEASE_ID + " FROM " + TABLE_DISEASE
                        + " ORDER BY " + DISEASE_ID + " DESC LIMIT 1;"); //Hacer query para obtener la clave primaria
                resultSet.first();
                int primaryKey = resultSet.getInt(1);

                for (Sintoma sintoma : enfermedad.getSintomas()) {
                    insertarSintomaYTipoSemantico(sintoma);
                    preparedStatementDiseaseHasSympton.setString(1, sintoma.getCodigoSintoma());
                    preparedStatementDiseaseHasSympton.setInt(2, primaryKey);

                    preparedStatementDiseaseHasSympton.executeUpdate();
                }

                for (Codigo codigo : enfermedad.getCodigos()) {
                    int id = insertarCodigo(codigo);
                    preparedStatementDiseaseHasCode.setInt(1, primaryKey);
                    preparedStatementDiseaseHasCode.setString(2, codigo.getCodigo());
                    preparedStatementDiseaseHasCode.setInt(3, id);
                    preparedStatementDiseaseHasCode.executeUpdate();
                }

            }
            mConnection.commit();
            mConnection.setAutoCommit(true);
        } catch (SQLException e) {
            System.err.println("Error al insertar los datos en la Base de datos");
            try {
                mConnection.rollback();
            } catch (SQLException e1) {
                System.err.println("Error al hacer rollback");
            }
        } finally {
            try {
                mConnection.setAutoCommit(true);
            } catch (SQLException e) {
                System.err.println("Error al reactivar auto commit");
            }
        }
    }

    /**
     * Realiza la inserción en la base de datos de un sintoma incluidos sus tipos semanticos
     *
     * @param sintoma El sintoma que se desea insertar en la base de datos
     */
    private void insertarSintomaYTipoSemantico(Sintoma sintoma) throws SQLException {

        //Inserción del sintoma


        PreparedStatement preparedQuery = mConnection.prepareStatement("SELECT  " + SYMPTON_CUI + " FROM "
                + TABLE_SYMPTON + " WHERE " + SYMPTON_CUI + " =? ");

        preparedQuery.setString(1, sintoma.getCodigoSintoma());
        ResultSet resultSet = preparedQuery.executeQuery();
        resultSet.last();
        if (resultSet.getRow() == 0) {//Comprobar si ya existe en la base de datos el CUI para no insertarlo de nuevo
            PreparedStatement preparedStatementSympton = mConnection.prepareStatement("INSERT INTO " + TABLE_SYMPTON + "("
                    + SYMPTON_CUI + "," + SYMPTON_NAME + ") VALUES (?,?)");

            preparedStatementSympton.setString(1, sintoma.getCodigoSintoma());
            preparedStatementSympton.setString(2, sintoma.getSintoma());

            preparedStatementSympton.executeUpdate();
        }

        //Insercion del semantic type
        PreparedStatement preparedStatementSemantycType = mConnection.prepareStatement("INSERT INTO " + TABLE_SEMANTIC_TYPE
                + "(" + SEMANTYC_TYPE_CUI + ") VALUES (?)", Statement.RETURN_GENERATED_KEYS);
        preparedStatementSemantycType.setString(1, sintoma.tipoSemantico);
        preparedStatementSemantycType.executeUpdate();
        ResultSet generatedKeys = preparedStatementSemantycType.getGeneratedKeys();
        generatedKeys.next();
        int foreignKey = generatedKeys.getInt(1);//Obtener la primary key generada

        PreparedStatement preparedStatementSymptonSemanticType = mConnection.prepareStatement("INSERT INTO " + TABLE_SYMPTON_SEMANTIC_TYPE
                + "(" + SYMPTON_CUI + "," + SEMANTYC_TYPE_ID + ") VALUES (?,?)");
        preparedStatementSymptonSemanticType.setString(1, sintoma.getCodigoSintoma());
        preparedStatementSymptonSemanticType.setInt(2, foreignKey);

        preparedStatementSymptonSemanticType.executeUpdate();


    }

    /**
     * Realiza la insercion de un codigo y en caso de que no exista el source realiza la inserción de este
     *
     * @param codigo Codigo que hay que insertar
     * @return Id del source
     */
    private int insertarCodigo(Codigo codigo) {
        try {
            PreparedStatement preparedStatementCode = mConnection.prepareStatement("INSERT INTO " + TABLE_CODE + "("
                    + CODE_ID + "," + SOURCE_ID + ") VALUES (?,?)");

            PreparedStatement queryStatement = mConnection.prepareStatement("SELECT  " + SOURCE_ID + " FROM " + TABLE_SOURCE
                    + " WHERE " + SOURCE_NAME + "=?");
            queryStatement.setString(1, codigo.getVocabulario());
            ResultSet resultSet = queryStatement.executeQuery();
            resultSet.last();
            if (resultSet.getRow() == 0) {
                PreparedStatement preparedStatementSource = mConnection.prepareStatement("INSERT INTO " + TABLE_SOURCE + "("
                        + SOURCE_NAME + ") VALUES (?)");
                preparedStatementSource.setString(1, codigo.getVocabulario());
                preparedStatementSource.executeUpdate();
                resultSet = queryStatement.executeQuery();
            }
            resultSet.first();
            int id = resultSet.getInt(1);

            preparedStatementCode.setString(1, codigo.getCodigo());
            preparedStatementCode.setInt(2, id);
            preparedStatementCode.executeUpdate();

            return id;

        } catch (SQLException e) {
            System.err.println("Error al insertar codigo en la Base de datos");
            return -1;
        }
    }


    public static void main(String args[]) {
        new Diagnostico().showMenu();
    }


    private class Enfermedad {
        private String nombre;
        private List<Codigo> codigos;
        private List<Sintoma> sintomas;

        public Enfermedad(String nombre, List<Codigo> codigos, List<Sintoma> sintomas) {
            this.nombre = nombre;
            this.codigos = codigos;
            this.sintomas = sintomas;
        }

        public String getNombre() {
            return nombre;
        }


        public List<Codigo> getCodigos() {
            return codigos;
        }


        public List<Sintoma> getSintomas() {
            return sintomas;
        }

    }

    private class Codigo {
        private String codigo;
        private String vocabulario;

        public Codigo(String codigo, String vocabulario) {
            this.codigo = codigo;
            this.vocabulario = vocabulario;
        }

        public String getCodigo() {
            return codigo;
        }


        public String getVocabulario() {
            return vocabulario;
        }

    }

    private class Sintoma {
        private String sintoma;
        private String codigoSintoma;
        private String tipoSemantico;

        public Sintoma(String sintoma, String codigoSintoma, String tipoSemantico) {
            this.sintoma = sintoma;
            this.codigoSintoma = codigoSintoma;
            this.tipoSemantico = tipoSemantico;
        }

        public String getSintoma() {
            return sintoma;
        }

        public String getCodigoSintoma() {
            return codigoSintoma;
        }

        public String getTipoSemantico() {
            return tipoSemantico;
        }
    }

}
