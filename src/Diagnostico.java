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
    private Connection mConnection;     //Variable global que contiene la conexi�n a la BD

    private final String SERVER = "localhost:3306";     //Direcci�n del servidor
    private final String USER = "bddx";                 //Usuario de la conexi�n
    private final String PASS = "bddx_pwd";             //Contrase�a de la conexi�n
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
            System.out.println("Bienvenido a sistema de diagn�stico\n");
            System.out.println("Selecciona una opci�n:\n");
            System.out.println("\t1. Creaci�n de base de datos y carga de datos.");
            System.out.println("\t2. Realizar diagn�stico.");
            System.out.println("\t3. Listar s�ntomas de una enfermedad.");
            System.out.println("\t4. Listar enfermedades y sus c�digos asociados.");
            System.out.println("\t5. Listar s�ntomas existentes en la BD y su tipo sem�ntico.");
            System.out.println("\t6. Mostrar estad�sticas de la base de datos.");
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
                System.err.println("Opci�n introducida no v�lida!");
            }
        } while (option != 7);
        exit();
    }

    private void exit() {
        System.out.println("Saliendo.. �hasta otra!");
        if (mConnection != null) {
            try {
                mConnection.close();    //Cerrar la conexi�n
                mConnection = null;     //Liberar el recurso
                System.out.println("Conexion cerrada");
            } catch (SQLException e) {
                System.err.println("Error cerrando la conexi�n");
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
        String url = CABECERA_CONEXION + TEST_URL + "/";   //Crear la URL de la conexi�n
        try {
            mConnection = DriverManager.getConnection(url, USER, PASS);     //Iniciar la conexion
        } catch (SQLException e) {
            System.err.println("Error al iniciar la conexi�n");
        }
        try {
            mConnection.setCatalog(NOMBRE_BASE_DE_DATOS);                   //Elegir la base de datos
        } catch (SQLException e) {
            System.err.println("No se pudo seleccionar la base de datos " + NOMBRE_BASE_DE_DATOS +
                    " por favor creela");
        }
        if (mConnection!=null)
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
        if (mConnection==null) return;
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

        System.out.println("Por favor introduzca el c�digo de los sintomas");
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
        if (mConnection==null) return;
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
        if (mConnection==null) return;
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
        if (checkIfIsConnected()) {
            conectar();
        }
        if (mConnection==null) return;
        System.out.println("Imprimiendo sintomas");
        System.out.println("\tCUI \t| Nombre \t | Tipo semantico");
        List<Sintoma> sintomas = getSintomas();
        if (sintomas == null || sintomas.size() == 0) {
            System.err.println("No se pudieron encontrar sinomas");
            return;
        }
        for (Sintoma sintoma : sintomas) {
            System.out.println("\t" + sintoma.getCodigoSintoma() + " \t" + sintoma.getSintoma() + "\t" + sintoma.getTipoSemantico());
        }
    }

    private void mostrarEstadisticasBD() {
        if (checkIfIsConnected()) {
            conectar();
        }
        if (mConnection==null) return;
        int numeroEnfermedades = getFilasDeTabla(TABLE_DISEASE);
        if (numeroEnfermedades == -1) System.err.println("Error obteniendo numero de enfermedades");
        else {
            System.out.println("Numero de enfermedades: " + numeroEnfermedades);
        }
        int numeroSintomas = getFilasDeTabla(TABLE_SYMPTON);
        System.out.print("\n");
        if (numeroSintomas == -1) System.err.println("Error al obtener el numero de sintomas");
        else {
            System.out.println("Numero de sintomas: " + numeroSintomas);
        }
        System.out.print("\n");
        String min = getEnfermedadMinSintomas();
        String max = getEnfermedadMaxSintomas();
        System.out.println(min + "\n" + max);
        double medio = numeroMedioDeSintomas();
        if (medio==-1)System.err.println("Error al obtener numero medio de sintomas por enfermedad");
        else{
            System.out.println("Numero medio de sintomas por enfermedad: " + medio);
        }
        System.out.print("\n");
        List<String> sintomasDeSemantycType = getNumeroSintomasDeSemantycType();
        System.out.println("Imprimiendo numero de sintomas de cada Semantyc type");
        if (sintomasDeSemantycType==null) System.err.println("Error obteniendo el numero de sintomas de cada semantyc type");
        else{
            for(String string:sintomasDeSemantycType){
                System.out.println(string);
            }
        }


    }

    /**
     * M�todo para leer n�meros enteros de teclado.
     *
     * @return Devuelve el n�mero le�do.
     * @throws Exception Puede lanzar excepci�n.
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
     * M�todo para leer cadenas de teclado.
     *
     * @return Devuelve la cadena le�da.
     * @throws Exception Puede lanzar excepci�n.
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
     * M�todo para leer el fichero que contiene los datos.
     *
     * @return Devuelve una lista de String con el contenido.
     * @throws Exception Puede lanzar excepci�n.
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
     * Comprueba si la base de datos existe en la conexi�n
     *
     * @param databaseName El nombre de la base de datos que se desea comprobar si existe
     * @return Devuelve cierto en caso de que la base de datos exista
     */
    private boolean checkIfDatabaseExists(String databaseName) {
        if (checkIfIsConnected()) {
            conectar();     //Si no existe la conexi�n crearla
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
     * Comprueba si la conexi�n existe y si esta abierta
     *
     * @return Devuelve falso en caso de que la conexi�n este correctamente establecida, cierto en cualquier otro caso
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
        System.out.println("�Quiere eliminarla y crearla de nuevo?\n");
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
            System.err.println("Opci�n introducida no v�lida!");
        }
    }

    /**
     * Realiza la eliminaci�n de la base de datos
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
            mConnection.setCatalog(NOMBRE_BASE_DE_DATOS); //Hacer que la conexi�n "apunte" a la base de datos
            crearTablas();
            insertarEnfermedadEnLaBD(parserDeDatos(readData()));
        } catch (SQLException e) {
            System.err.println("Error creando la base de datos");
        } catch (Exception e) {
            System.err.println("Error al leer el archivo de datos");
        }
    }

    /**
     * Creaci�n de las tablas en la Base de datos
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
     * @param strings Lista que contiene las lineas con la informaci�n de cada enfermedad
     * @return Lista enlazada de objetos enfermedad dentro de los cuales est� toda la informaci�n
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
                resultSet.first();
                int primaryKey = resultSet.getInt(1);


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
                resultSet.close();
                preparedStatementDisease.close();

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
     * Realiza la inserci�n en la base de datos de un sintoma incluidos sus tipos semanticos
     *
     * @param sintoma El sintoma que se desea insertar en la base de datos
     */
    private void insertarSintomaYTipoSemantico(Sintoma sintoma) throws SQLException {

        //Inserci�n del sintoma

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
        } catch (SQLException e) {//Si ya est� en la BD buscar la clave
            PreparedStatement preparedStatement = mConnection.prepareStatement("SELECT " + SEMANTYC_TYPE_ID + " FROM "
                    + TABLE_SEMANTIC_TYPE + " WHERE " + SEMANTYC_TYPE_CUI + "=?");
            preparedStatement.setString(1, sintoma.getTipoSemantico());
            ResultSet clave = preparedStatement.executeQuery();
            clave.first();
            foreignKey = clave.getInt(1);

        }

        try {
            PreparedStatement preparedStatementSymptonSemanticType = mConnection.prepareStatement("INSERT INTO "
                    + TABLE_SYMPTON_SEMANTIC_TYPE
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
     * Realiza la insercion de un codigo y en caso de que no exista el source realiza la inserci�n de este
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
                    + " JOIN " + TABLE_SEMANTIC_TYPE + " ON " + TABLE_SEMANTIC_TYPE + "." + SEMANTYC_TYPE_ID
                    + "=" + TABLE_SYMPTON_SEMANTIC_TYPE + "." + SEMANTYC_TYPE_ID);
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
                    + SOURCE_ID + " WHERE " + DISEASE_ID + "=?");
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

    /**
     * Realiza una query que busca del numero de elementos que hay en una tabla
     *
     * @param tabla Tabla de la que se quiere obtener el numero de elementos
     * @return Devuelve el numero de enfermedades en la BD, en caso de haber un error devuelve -1
     */
    private int getFilasDeTabla(String tabla) {
        try {
            Statement statement = mConnection.createStatement(); //No es necesario usar
            //un PreparedStatement debido a que el parameto es pasado por nosotros
            ResultSet resultSet = statement.executeQuery("SELECT COUNT(*) FROM " + tabla);
            resultSet.next();
            int numero = resultSet.getInt(1);
            statement.close();
            resultSet.close();
            return numero;
        } catch (SQLException e) {
            System.err.println("Error al obtener numero de elementos");
            return -1;
        }
    }

    /**
     * Realiza la busqueda en la BD de la enfermedad con mas sintomas
     *
     * @return String que contiene el nombre de la enfermedad con mas sintomas y cuantos son
     */
    private String getEnfermedadMaxSintomas() {
        try {
            Statement statement = mConnection.createStatement();
            ResultSet resultSet = statement.executeQuery("SELECT " + TABLE_DISEASE + "." + DISEASE_NAME
                    + ", COUNT(*) AS conteo FROM " + TABLE_DISEASE_SYMPTON + " JOIN " + TABLE_DISEASE + " ON "
                    + TABLE_DISEASE_SYMPTON + "." + DISEASE_ID + "=" + TABLE_DISEASE + "." + DISEASE_ID
                    + " GROUP BY " + DISEASE_NAME + " ORDER BY conteo DESC ");

            resultSet.next();
            String sol = "La enfermedad con mas sintomas es: " + resultSet.getString(1) + " con "
                    + resultSet.getInt(2) + " sintomas";
            resultSet.close();
            statement.close();
            return sol;
        } catch (SQLException e) {
            System.err.println("Error al obtener el maximo");
            return null;
        }
    }

    /**
     * Realiza la busqueda en la BD de la enfermedad con menos sintomas
     *
     * @return String que contiene el nombre de la enfermedad con menos sintomas y cuantos son
     */
    private String getEnfermedadMinSintomas() {
        try {
            Statement statement = mConnection.createStatement();
            ResultSet resultSet = statement.executeQuery("SELECT " + TABLE_DISEASE + "." + DISEASE_NAME
                    + ", COUNT(*) AS conteo FROM " + TABLE_DISEASE_SYMPTON + " JOIN " + TABLE_DISEASE + " ON "
                    + TABLE_DISEASE_SYMPTON + "." + DISEASE_ID + "=" + TABLE_DISEASE + "." + DISEASE_ID
                    + " GROUP BY " + DISEASE_NAME + " ORDER BY conteo ASC ");

            resultSet.next();
            String sol = "La enfermedad con menos sintomas es: " + resultSet.getString(1) + " con "
                    + resultSet.getInt(2) + " sintomas";
            resultSet.close();
            statement.close();
            return sol;
        } catch (SQLException e) {
            System.err.println("Error al obtener el maximo");
            return null;
        }
    }

    /**
     * Realiza el calculo del numero medio de sintomas por enfermedad en las BD
     * @return Numero medio de sintomas por enfermedad, devuelve -1 en caso de error
     */
    private double numeroMedioDeSintomas() {
        try {
            Statement statement = mConnection.createStatement();
            ResultSet resultSet = statement.executeQuery("SELECT AVG(conteo) FROM (SELECT COUNT(*) as conteo FROM "
                    + TABLE_DISEASE_SYMPTON + " GROUP BY "+ DISEASE_ID + ")tconteo");
            resultSet.next();
            double sol = resultSet.getDouble(1);
            resultSet.close();
            statement.close();
            return sol;
        } catch (SQLException e) {
            System.err.println("Error al obtener media");
            return -1;
        }
    }

    /**
     * Hace una query en la BD para ver cuantos sintomas tiene cada Semantyc type asignados
     * @return Lista de Strings con la informacion de la query
     */
    private List<String> getNumeroSintomasDeSemantycType (){
        try {
            Statement statement = mConnection.createStatement();
            ResultSet resultSet = statement.executeQuery("SELECT COUNT(*), " + TABLE_SEMANTIC_TYPE+"."+SEMANTYC_TYPE_CUI
                    + " FROM " + TABLE_SYMPTON_SEMANTIC_TYPE + " JOIN " + TABLE_SEMANTIC_TYPE + " ON "
                    + TABLE_SEMANTIC_TYPE+"."+SEMANTYC_TYPE_ID + "=" + TABLE_SYMPTON_SEMANTIC_TYPE+"."+SEMANTYC_TYPE_ID
                    + " GROUP BY " + SEMANTYC_TYPE_CUI);
            List<String> sol = new LinkedList<>();
            while (resultSet.next()){
                sol.add(resultSet.getString(2) + " tiene: " + resultSet.getInt(1) + " sintomas");
            }
            return sol;

        } catch (SQLException e) {
            System.err.println("Error al obtener el numero de sintomas de casa semantyc type");
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
