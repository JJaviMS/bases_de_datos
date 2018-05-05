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
        if (checkIfIsConnected()) {
            conectar();
        }
        System.out.println("Imprimiendo por pantalla los sintomas");
        List<Sintoma> sintomas = getSintomas();
        if (sintomas == null) {
            System.out.println("Hubo un error al obtener los sintomas");
            return;
        }
        System.out.println("\tCodigo  | Sintoma");
        for (Sintoma sintoma : sintomas) {
            System.out.println("\t" + sintoma.getCodigoSintoma() + "  " + sintoma.getSintoma());
        }

        System.out.println("Por favor introduzca el código de los sintomas");
        System.out.println("Cuando haya finalizado pulse intro");
        List<String> busqueda = new LinkedList<>();
        String valor;
        do {
            try {
                valor = readString();
            } catch (Exception e) {
                System.err.println("Error al leer por teclado");
                valor = "";
                continue;
            }
            if (!valor.isEmpty()) {
                busqueda.add(valor);
            }
        } while (!valor.isEmpty());

        List<Enfermedad> enfermedades = buscarEnfermedadConSintomas(busqueda);
        if (enfermedades == null || enfermedades.size() == 0) {
            System.out.println("No se pudieron encontrar enfermedades que cumplieran todos los sintomas proporcionados");
            return;
        }
        for (Enfermedad enfermedad : enfermedades) {
            System.out.println("\t" + enfermedad.getNombre());
        }

    }

    private void listarSintomasEnfermedad() {
        if (checkIfIsConnected()) {
            conectar();
        }
        System.out.println("Imprimiendo enfermedades");
        List<Enfermedad> enfermedades = getEnfermedadesDeLaBd();
        if (enfermedades == null || enfermedades.size() == 0) {
            System.err.println("Error obteniendo las enfermedades");
            return;
        }
        System.out.println("\tCodigo  | Enfermedad");
        for (Enfermedad enfermedad : enfermedades) {
            System.out.println("\t" + enfermedad.getId() + "\t\t  " + enfermedad.getNombre());
        }
        System.out.println("Por favor introduce el ID de la enfermedad que desea consultar");
        String id = null;
        try {
            id = readString();

        } catch (Exception e) {
            System.err.println("Error al imprimir al leer el ID");
        }
        List<Sintoma> sintomas = getSintomas(id);
        if (sintomas == null || sintomas.size() == 0) {
            System.out.println("No hay sintomas que mostrar");
            return;
        }
        for (Sintoma sintoma : sintomas) {
            System.out.println("\t" + sintoma.getSintoma());
        }

    }

    private void listarEnfermedadesYCodigosAsociados() {
        if (checkIfIsConnected()) {
            conectar();
        }
        System.out.println("Imprimiendo enfermedades");
        List<Enfermedad> enfermedades = getEnfermedadesDeLaBd();
        if (enfermedades == null || enfermedades.size() == 0) {
            System.err.println("Error obteniendo las enfermedades");
            return;
        }
        for (Enfermedad enfermedad : enfermedades) {
            enfermedad.setCodigos(getCodigos(enfermedad.getId()));
        }
        for (Enfermedad enfermedad : enfermedades) {
            System.out.println("Enfermedad: " + enfermedad.getId() + " - " + enfermedad.getNombre());
            System.out.println("\n");
            System.out.println("Codigos:");
            System.out.println("\tCodigo  \t| Source");
            for (Codigo codigo : enfermedad.getCodigos()) {
                System.out.println("\t" + codigo.getCodigo() + " \t\t " + codigo.getVocabulario());
            }
            System.out.println("\n");
        }
    }

    private void listarSintomasYTiposSemanticos() {
        if (checkIfIsConnected()){
            conectar();
        }
        System.out.println("Imprimiendo sintomas");
        System.out.println("\tCUI \t| Nombre \t | Tipo semantico");
        List<Sintoma> sintomas = getSintomas();
        if (sintomas==null ||sintomas.size()==0){
            System.err.println("No se pudieron encontrar sinomas");
            return;
        }
        for (Sintoma sintoma : sintomas){
            System.out.println("\t" + sintoma.getCodigoSintoma() + " \t" + sintoma.getSintoma() + "\t" + sintoma.getTipoSemantico());
        }
    }

    private void mostrarEstadisticasBD() {
        try {
             Statement statement = mConnection.createStatement();    /*En este caso por dependencia de metodos por encima
              no es necesario comprobar la conexion*/
             ResultSet numeroEnfermedades = 
             //Número de enfermedades: Un conteo del número total deenfermedades que hay en la base de datos.
             statement.executeQuery("EL NUMERO TOTAL DE ENFERMEDADES EN BASE DE DATOS: "+
              "SELECT COUNT(*) FROM" + TABLE_DISEASE);
             numeroEnfermedades.getInt(1);
             System.out.println("EL NUMERO TOTAL DE ENFERMEDADES IMPRIMIDOS CORRECTAMENTE");
             
             //Número de síntomas: Un conteo del número total de síntomas que hay en la base de datos.
             ResultSet numeroSintomas=
             statement.executeQuery("EL NUMERO TOTAL DE SINTOMAS EN BASE DE DATOS: "+
                     "SELECT COUNT(*) FROM" + TABLE_DISEASE_SYMPTON);
             numeroSintomas.getInt(1);
             System.out.println("EL NUMERO TOTAL DE SINTOMAS IMPRIMIDOS CORRECTAMENTE");
             
             /*Enfermedad con más síntomas, con menos síntomas y número medio de
             síntomas [0.5 puntos]: Debe indicar cuales son las enfermedades con más y
             menos síntomas y cuál es el número medio de síntomas asociados a las
             enfermedades.*/
             ResultSet enferSintomasMax=
             statement.executeQuery("LA ENFERMEDAD CON MÁS SÍNTOMAS: "+ "SELECT "+DISEASE_ID+", count(SYMPTON_CUI) TOPENFERMEDAD, "+
            		 DISEASE_NAME+"FROM"+TABLE_DISEASE_SYMPTON +" , "+TABLE_DISEASE+"WHERE "+TABLE_DISEASE_SYMPTON+"."+ DISEASE_ID
            		 +"="+TABLE_DISEASE+"."+DISEASE_ID+"GROUP BY "+TABLE_DISEASE_SYMPTON+"."+DISEASE_ID+"ORDER BY TOPENFERMEDAD DESC "+
            		 "LIMIT 1 ");
             numeroSintomas.getString(3);
             System.out.println("LA ENFERMEDAD CON MÁS SINTOMAS HA IMPRIMIDO CORRECTAMENTE");
             
             ResultSet enferSintomasMin=
                     statement.executeQuery("LA ENFERMEDAD CON MENOS SÍNTOMAS: "+ "SELECT "+DISEASE_ID+", count(SYMPTON_CUI) TOPENFERMEDAD, "+
                    		 DISEASE_NAME+"FROM"+TABLE_DISEASE_SYMPTON +" , "+TABLE_DISEASE+"WHERE "+TABLE_DISEASE_SYMPTON+"."+ DISEASE_ID
                    		 +"="+TABLE_DISEASE+"."+DISEASE_ID+"GROUP BY "+TABLE_DISEASE_SYMPTON+"."+DISEASE_ID+"ORDER BY TOPENFERMEDAD ASC "+
                    		 "LIMIT 1 ");
             numeroSintomas.getString(3);
             System.out.println("LA ENFERMEDAD CON MENOS SINTOMAS HA IMPRIMIDO CORRECTAMENTE");

             
                 
         } catch (SQLException e) {
             System.err.println("Error eliminando en mostrarEstadisticasBD");
         }
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
        LinkedList<String> data = new LinkedList<>();
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
        if (checkIfIsConnected()) {
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
     * @return Devuelve falso en caso de que la conexión este correctamente establecida, cierto en cualquier otro caso
     */
    private boolean checkIfIsConnected() {
        try {
            return mConnection == null || mConnection.isClosed();
        } catch (SQLException e) {
            System.err.println("Error al comprobar el estado de la conexion");
            return true;
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
                    + " VARCHAR(45) UNIQUE , PRIMARY KEY (" + SEMANTYC_TYPE_ID + "));");


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

    /**
     * Dada una lista de enfermedades realiza la insercion de dicha enfermedad en la BD junto con sus codigos y sintomas
     *
     * @param enfermedades Las enfermedades que se quieren introducir en la BD
     */
    private void insertarEnfermedadEnLaBD(LinkedList<Enfermedad> enfermedades) {
        try {
            mConnection.setAutoCommit(false);

            for (Enfermedad enfermedad : enfermedades) {
                PreparedStatement preparedStatementDisease = mConnection.prepareStatement("INSERT INTO " + TABLE_DISEASE + " (" + DISEASE_NAME
                        + ") VALUES (?)", Statement.RETURN_GENERATED_KEYS);

                preparedStatementDisease.setString(1, enfermedad.getNombre());
                preparedStatementDisease.executeUpdate();
                ResultSet resultSet = preparedStatementDisease.getGeneratedKeys();
                preparedStatementDisease.close();
                resultSet.first();
                int primaryKey = resultSet.getInt(1);
                resultSet.close();

                for (Sintoma sintoma : enfermedad.getSintomas()) {
                    insertarSintomaYTipoSemantico(sintoma);
                    PreparedStatement preparedStatementDiseaseHasSympton = mConnection.prepareStatement("INSERT INTO " + TABLE_DISEASE_SYMPTON
                            + "(" + SYMPTON_CUI + "," + DISEASE_ID + ") VALUES (?,?)");

                    preparedStatementDiseaseHasSympton.setString(1, sintoma.getCodigoSintoma());
                    preparedStatementDiseaseHasSympton.setInt(2, primaryKey);

                    preparedStatementDiseaseHasSympton.executeUpdate();
                    preparedStatementDiseaseHasSympton.close();
                }
                for (Codigo codigo : enfermedad.getCodigos()) {
                    int id = insertarCodigo(codigo);
                    PreparedStatement preparedStatementDiseaseHasCode = mConnection.prepareStatement("INSERT INTO " + TABLE_DISEASE_HAS_CODE + "("
                            + DISEASE_ID + "," + CODE_ID + "," + SOURCE_ID + ") VALUES (?,?,?)");
                    preparedStatementDiseaseHasCode.setInt(1, primaryKey);
                    preparedStatementDiseaseHasCode.setString(2, codigo.getCodigo());
                    preparedStatementDiseaseHasCode.setInt(3, id);
                    preparedStatementDiseaseHasCode.executeUpdate();
                    preparedStatementDiseaseHasCode.close();
                }

            }
            mConnection.commit();
        } catch (SQLException e) {
            System.err.println("Error al insertar los datos en la Base de datos");
            e.printStackTrace();
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

        try {
            PreparedStatement preparedStatementSympton = mConnection.prepareStatement("INSERT INTO " + TABLE_SYMPTON + "("
                    + SYMPTON_CUI + "," + SYMPTON_NAME + ") VALUES (?,?)");

            preparedStatementSympton.setString(1, sintoma.getCodigoSintoma());
            preparedStatementSympton.setString(2, sintoma.getSintoma());

            preparedStatementSympton.executeUpdate();
            preparedStatementSympton.close();
            //Si lanza excepcion de SQL es que el sintoma ya esta presente en la BD
        } catch (SQLException e) {
            //No hacer nada ya que el sintoma ya esta en la BD
        }

        //Insercion del semantic type
        int foreignKey;
        try {
            PreparedStatement preparedStatementSemantycType = mConnection.prepareStatement("INSERT INTO " + TABLE_SEMANTIC_TYPE
                    + "(" + SEMANTYC_TYPE_CUI + ") VALUES (?)", Statement.RETURN_GENERATED_KEYS);
            preparedStatementSemantycType.setString(1, sintoma.tipoSemantico);
            preparedStatementSemantycType.executeUpdate();
            ResultSet generatedKeys = preparedStatementSemantycType.getGeneratedKeys();
            preparedStatementSemantycType.close();
            generatedKeys.next();
            foreignKey = generatedKeys.getInt(1);//Obtener la primary key generada
            generatedKeys.close();
        } catch (SQLException e) {//Si ya está en la BD buscar la clave
            PreparedStatement preparedStatement = mConnection.prepareStatement("SELECT " + SEMANTYC_TYPE_ID + " FROM "
                    + TABLE_SEMANTIC_TYPE + " WHERE " + SEMANTYC_TYPE_CUI + "=?");
            preparedStatement.setString(1, sintoma.getTipoSemantico());
            ResultSet clave = preparedStatement.executeQuery();
            preparedStatement.close();
            clave.first();
            foreignKey = clave.getInt(1);
            clave.close();
        }

        try {
            PreparedStatement preparedStatementSymptonSemanticType = mConnection.prepareStatement("INSERT INTO " + TABLE_SYMPTON_SEMANTIC_TYPE
                    + "(" + SYMPTON_CUI + "," + SEMANTYC_TYPE_ID + ") VALUES (?,?)");
            preparedStatementSymptonSemanticType.setString(1, sintoma.getCodigoSintoma());
            preparedStatementSymptonSemanticType.setInt(2, foreignKey);

            preparedStatementSymptonSemanticType.executeUpdate();
            preparedStatementSymptonSemanticType.close();
        } catch (SQLException e) {
            //Si ya esta en la BD no hacer nada
        }


    }

    /**
     * Realiza la insercion de un codigo y en caso de que no exista el source realiza la inserción de este
     *
     * @param codigo Codigo que hay que insertar
     * @return Id del source
     */
    private int insertarCodigo(Codigo codigo) throws SQLException {

        PreparedStatement preparedStatementCode = mConnection.prepareStatement("INSERT INTO " + TABLE_CODE + "("
                + CODE_ID + "," + SOURCE_ID + ") VALUES (?,?)");


        int id;
        try {
            PreparedStatement preparedStatementSource = mConnection.prepareStatement("INSERT INTO " + TABLE_SOURCE + "("
                    + SOURCE_NAME + ") VALUES (?)", Statement.RETURN_GENERATED_KEYS);
            preparedStatementSource.setString(1, codigo.getVocabulario());
            preparedStatementSource.executeUpdate();

            ResultSet keys = preparedStatementSource.getGeneratedKeys();
            keys.first();
            id = keys.getInt(1); //Obtener la clave generada
        } catch (SQLException e) { //Si ha llegado a catch es que ya existia el valor en la BD y debe buscar su clave en ella
            PreparedStatement queryStatement = mConnection.prepareStatement("SELECT  " + SOURCE_ID + " FROM " + TABLE_SOURCE
                    + " WHERE " + SOURCE_NAME + "=?");
            queryStatement.setString(1, codigo.getVocabulario());
            ResultSet resultSet = queryStatement.executeQuery();
            resultSet.last();

            id = resultSet.getInt(1);
        }

        preparedStatementCode.setString(1, codigo.getCodigo());
        preparedStatementCode.setInt(2, id);
        preparedStatementCode.executeUpdate();
        preparedStatementCode.close();

        return id;
    }

    /**
     * Realiza una busqueda en la BD de todos los sintomas
     *
     * @return Una lista que contiene todos los sintomas de la BD
     */
    private List<Sintoma> getSintomas() {
        List<Sintoma> sintomas = new LinkedList<>();

        try {
            Statement statement = mConnection.createStatement();
            ResultSet resultSet = statement.executeQuery("SELECT " + TABLE_SYMPTON + "." + SYMPTON_CUI + ","
                    + TABLE_SYMPTON + "." + SYMPTON_NAME + "," + TABLE_SEMANTIC_TYPE + "." + SEMANTYC_TYPE_CUI
                    + " FROM " + TABLE_SYMPTON
                    + " JOIN " + TABLE_SYMPTON_SEMANTIC_TYPE + " ON " + TABLE_SYMPTON_SEMANTIC_TYPE + "." + SYMPTON_CUI
                    + "=" + TABLE_SYMPTON + "." + SYMPTON_CUI
                    + " JOIN " + TABLE_SEMANTIC_TYPE +" ON "+TABLE_SEMANTIC_TYPE + "."+SEMANTYC_TYPE_ID
                    + "=" + TABLE_SYMPTON_SEMANTIC_TYPE +"."+SEMANTYC_TYPE_ID);
            while (resultSet.next()) {
                sintomas.add(new Sintoma(resultSet.getString(1), resultSet.getString(2), resultSet.getString(3)));
            }
            resultSet.close();
            return sintomas;
        } catch (SQLException e) {
            System.err.println("Error al obtener los sintomas de la base de datos");
            return null;
        }
    }

    /**
     * Dada una lista de sintomas devuelve una lista de enfermedades las cuales contengan todos los sintomas
     *
     * @param sintomas Sintomas de los cuales se quieran buscar las enfermedades
     * @return Lista con las enfermedades que contengan los sintomas
     */
    private List<Enfermedad> buscarEnfermedadConSintomas(List<String> sintomas) {
        try {
            StringBuilder builder = new StringBuilder();
            for (int i = 0; i < sintomas.size(); i++) {
                builder.append("?,");
            }//Con este bucle se ponen tantas interrogaciones (?) como sintomas se vayan a buscar
            builder.deleteCharAt(builder.length() - 1);


            PreparedStatement preparedStatement = mConnection.prepareStatement("SELECT " + DISEASE_NAME + " FROM " + TABLE_DISEASE
                    + " JOIN " + TABLE_DISEASE_SYMPTON + " ON " + TABLE_DISEASE + "." + DISEASE_ID + "=" + TABLE_DISEASE_SYMPTON + "." + DISEASE_ID
                    + " WHERE " + SYMPTON_CUI + " IN (" + builder.toString() + ") GROUP BY " + DISEASE_NAME
                    + " HAVING COUNT(?)");
            //Juntar las tablas de sympton y disease para tener el nombre y agruparlas por nombre
            //Solo mostrar los conjuntos los cuales tengan el numero de sintomas introducidos
            int index = 1;
            for (String sintoma : sintomas) {
                preparedStatement.setString(index++, sintoma);
            }
            preparedStatement.setInt(index, sintomas.size());
            ResultSet resultSet = preparedStatement.executeQuery();

            List<Enfermedad> enfermedades = new LinkedList<>();
            while (resultSet.next()) {
                enfermedades.add(new Enfermedad(resultSet.getString(1), null, null));
            }
            resultSet.close();
            preparedStatement.close();
            return enfermedades;
        } catch (SQLException e) {
            System.err.println("Error buscando la enfermedad");
            e.printStackTrace();
            return null;
        }
    }

    /**
     * Devuelve los codigos asignados a la clave de una enfermedad
     *
     * @param id La clave de la enfermedad que se desea obtener sus codigos
     * @return Lista de objetos de la clase Codigo los cuales se corresponden con la enfermedad
     */
    private List<Codigo> getCodigos(int id) {
        try {
            PreparedStatement preparedStatement = mConnection.prepareStatement("SELECT " + TABLE_DISEASE_HAS_CODE + "." + CODE_ID
                    + "," + TABLE_SOURCE + "." + SOURCE_NAME + " FROM " + TABLE_DISEASE_HAS_CODE
                    + " JOIN " + TABLE_SOURCE + " ON " + TABLE_SOURCE + "." + SOURCE_ID + "=" + TABLE_DISEASE_HAS_CODE + "."
                    + SOURCE_ID + " WHERE " + DISEASE_ID + "=?" + " ORDER BY " + TABLE_SYMPTON+"."+SYMPTON_CUI + " ASC");
            preparedStatement.setInt(1, id);
            ResultSet resultSet = preparedStatement.executeQuery();
            List<Codigo> codigos = new LinkedList<>();
            while (resultSet.next()) {
                codigos.add(new Codigo(resultSet.getString(1), resultSet.getString(2)));
            }
            resultSet.close();
            preparedStatement.close();
            return codigos;
        } catch (SQLException e) {
            System.err.println("Error obteniendo los codigos");
            return null;
        }
    }

    /**
     * Realiza la extraccion de todas las enfermedades de la BD
     *
     * @return Lista con todas las enfermedades de la BD
     */
    private List<Enfermedad> getEnfermedadesDeLaBd() {
        try {
            Statement statement = mConnection.createStatement();
            List<Enfermedad> enfermedades = new LinkedList<>();

            ResultSet resultSet = statement.executeQuery("SELECT * FROM " + TABLE_DISEASE + " ORDER BY " + DISEASE_ID + " ASC");
            while (resultSet.next()) {
                enfermedades.add(new Enfermedad(resultSet.getString(2), resultSet.getInt(1)));
            }
            resultSet.close();
            statement.close();
            return enfermedades;
        } catch (SQLException e) {
            System.err.println("Error al obtener las enfermedades");
            return null;
        }
    }

    /**
     * Dada la id de una enfermedad devuelve todos los sintomas asociados a ella
     *
     * @param id Clave primaria de la enfermedad
     * @return Lista con los sintomas asociados a la enfermedad
     */
    private List<Sintoma> getSintomas(String id) {
        List<Sintoma> sintomas = new LinkedList<>();
        try {
            PreparedStatement preparedStatement = mConnection.prepareStatement("SELECT " + SYMPTON_NAME + " FROM "
                    + TABLE_DISEASE_SYMPTON + " JOIN " + TABLE_SYMPTON + " ON " + TABLE_DISEASE_SYMPTON + "." + SYMPTON_CUI
                    + "=" + TABLE_SYMPTON + "." + SYMPTON_CUI + " WHERE " + DISEASE_ID + "=?");

            preparedStatement.setString(1, id);
            ResultSet resultSet = preparedStatement.executeQuery();
            while (resultSet.next()) {
                sintomas.add(new Sintoma(resultSet.getString(1), null, null));
            }
            resultSet.close();
            preparedStatement.close();
            return sintomas;
        } catch (SQLException e) {
            System.err.println("Error al obtener los sintomas");
            return null;
        }
    }


    public static void main(String args[]) {
        new Diagnostico().showMenu();
    }


    private class Enfermedad {
        private String nombre;
        private List<Codigo> codigos;
        private List<Sintoma> sintomas;
        private Integer id;

        Enfermedad(String nombre, List<Codigo> codigos, List<Sintoma> sintomas) {
            this.nombre = nombre;
            this.codigos = codigos;
            this.sintomas = sintomas;
        }

        Enfermedad(String nombre, Integer id) {
            this.nombre = nombre;
            this.id = id;
        }

        String getNombre() {
            return nombre;
        }


        List<Codigo> getCodigos() {
            return codigos;
        }


        List<Sintoma> getSintomas() {
            return sintomas;
        }

        Integer getId() {
            return id;
        }

        void setNombre(String nombre) {
            this.nombre = nombre;
        }

        void setCodigos(List<Codigo> codigos) {
            this.codigos = codigos;
        }

        void setSintomas(List<Sintoma> sintomas) {
            this.sintomas = sintomas;
        }

        void setId(Integer id) {
            this.id = id;
        }
    }

    private class Codigo {
        private String codigo;
        private String vocabulario;

        Codigo(String codigo, String vocabulario) {
            this.codigo = codigo;
            this.vocabulario = vocabulario;
        }

        String getCodigo() {
            return codigo;
        }


        String getVocabulario() {
            return vocabulario;
        }

    }

    private class Sintoma {
        private String sintoma;
        private String codigoSintoma;
        private String tipoSemantico;

        Sintoma(String sintoma, String codigoSintoma, String tipoSemantico) {
            this.sintoma = sintoma;
            this.codigoSintoma = codigoSintoma;
            this.tipoSemantico = tipoSemantico;
        }

        String getSintoma() {
            return sintoma;
        }

        String getCodigoSintoma() {
            return codigoSintoma;
        }

        String getTipoSemantico() {
            return tipoSemantico;
        }
    }

}
