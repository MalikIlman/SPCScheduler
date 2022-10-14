Imports System.Data.SqlClient
Public Class clsSchedulerDB
    Public Shared Function Validate(ByVal pFlag As Integer, ByVal pConStr As String) As DataTable
        Dim da As SqlDataAdapter
        Dim dt As New DataTable
        Dim sql As String
        Dim con As New SqlConnection
        Dim SQLTrans As SqlTransaction
        Dim cmd As New SqlCommand

        con = New SqlConnection(pConStr)
        con.Open()

        'SQLTrans = con.BeginTransaction
        Try
            sql = "sp_OGIScheduler_Validate"

            cmd = New SqlCommand
            cmd.CommandText = sql
            cmd.Connection = con
            cmd.CommandType = CommandType.StoredProcedure
            cmd.Transaction = SQLTrans

            cmd.Parameters.AddWithValue("@Flag", pFlag)
            da = New SqlDataAdapter(cmd)
            da.Fill(dt)
            cmd.Parameters.Clear()
            cmd.Dispose()
            'SQLTrans.Commit()

            Return dt
        Catch ex As Exception
            SQLTrans.Rollback()
            Return Nothing
            Throw New Exception("clsScheduler.Validate : " & ex.Message)
        Finally
            con.Close()
        End Try
    End Function

    Public Shared Function GetClsUnit(ByVal pCode As String, ByVal pType As String, ByVal pConStr As String) As DataTable
        Dim da As SqlDataAdapter
        Dim dt As New DataTable
        Dim con As New SqlConnection
        Dim SQLTrans As SqlTransaction
        Dim cmd As New SqlCommand
        Dim sql As String

        con = New SqlConnection(pConStr)
        con.Open()

        'SQLTrans = con.BeginTransaction

        Try
            sql = "sp_OGIScheduler_Validate"

            cmd = New SqlCommand
            cmd.CommandText = sql
            cmd.Connection = con
            cmd.Transaction = SQLTrans
            cmd.CommandType = CommandType.StoredProcedure

            cmd.Parameters.AddWithValue("@Param1", pCode)
            cmd.Parameters.AddWithValue("@Type", pType)
            da = New SqlDataAdapter(cmd)
            da.Fill(dt)
            cmd.Parameters.Clear()
            cmd.Dispose()
            'SQLTrans.Commit()

            Return dt
        Catch ex As Exception
            SQLTrans.Rollback()
            Throw New Exception("Get Data List Sales Return Header --> " & ex.Message)
        Finally
            con.Close()
        End Try

    End Function

    Public Shared Function Scheduler_Delete(ByRef iFlag As Integer, ByVal pConStr As String, Optional ByRef pErr As String = "") As Integer
        Dim i As Integer
        Dim con As New SqlConnection
        Dim SQLTrans As SqlTransaction
        Dim cmd As New SqlCommand

        con = New SqlConnection(pConStr)
        con.Open()

        ''SQLTrans = con.BeginTransaction

        Try
            Dim sql As String = ""

            sql = "sp_OGIScheduler_Del"

            cmd = New SqlCommand
            cmd.CommandText = sql
            cmd.Connection = con
            cmd.Transaction = SQLTrans
            cmd.CommandType = CommandType.StoredProcedure

            cmd.Parameters.AddWithValue("@Flag", iFlag)
            i = cmd.ExecuteNonQuery

            ''SQLTrans.Commit()

            Return i

        Catch ex As SqlException
            SQLTrans.Rollback()
            pErr = ex.Message
            Return Nothing
        Finally
            con.Close()
        End Try
    End Function
    Public Shared Function Import_Picking_List(ByVal pConStr As String, Optional ByRef pErr As String = "") As Boolean
        Dim i As Integer
        Dim con As New SqlConnection
        Dim SQLTrans As SqlTransaction
        Dim cmd As SqlCommand

        con = New SqlConnection(pConStr)
        con.Open()

        SQLTrans = con.BeginTransaction
        Try
            Dim sql As String = ""
            sql = "SP_OgiScheduler_PickingList_Import_Insert"

            cmd = New SqlCommand
            cmd.CommandText = sql
            cmd.Connection = con
            cmd.Transaction = SQLTrans
            cmd.CommandType = CommandType.StoredProcedure
            i = cmd.ExecuteNonQuery
            SQLTrans.Commit()

            Return True

        Catch ex As SqlException
            SQLTrans.Rollback()
            pErr = ex.Message
            Return False
        Finally
            con.Close()
        End Try
    End Function
    Public Shared Function Scheduler_Insert_Main(ByRef iFlag As Integer, ByVal pConStr As String, Optional ByRef pErr As String = "") As Boolean
        Dim i As Integer
        Dim con As New SqlConnection
        Dim SQLTrans As SqlTransaction
        Dim cmd As SqlCommand

        con = New SqlConnection(pConStr)
        con.Open()

        SQLTrans = con.BeginTransaction
        Try
            Dim sql As String = ""
            sql = "sp_OGIScheduler_Rcv_Ins"

            cmd = New SqlCommand
            cmd.CommandText = sql
            cmd.Connection = con
            cmd.Transaction = SQLTrans
            cmd.CommandType = CommandType.StoredProcedure

            cmd.Parameters.AddWithValue("@Flag", iFlag)
            i = cmd.ExecuteNonQuery
            SQLTrans.Commit()

            Return True

        Catch ex As SqlException
            SQLTrans.Rollback()
            pErr = ex.Message
            Return False
        Finally
            con.Close()
        End Try
    End Function

    Public Shared Function Insert_Item_Master(ByVal pConStr As String, ByVal pMaster As clsScheduler, Optional ByRef pErr As String = "") As Boolean

        Dim cmd As SqlCommand
        Dim sql As String
        Dim status As Boolean = False
        Dim con As New SqlConnection
        Dim SQLTrans As SqlTransaction
        Dim LastUpdate As DateTime

        con = New SqlConnection(pConStr)
        con.Open()

        'SQLTrans = con.BeginTransaction
        Try

            LastUpdate = DateTime.ParseExact(pMaster.val_45, "MM/dd/yyyy", Nothing)

            sql = "sp_OGIScheduler_Item_Master_Ins"

            cmd = New SqlCommand
            cmd.CommandText = sql
            cmd.Connection = con
            cmd.Transaction = SQLTrans
            cmd.CommandType = CommandType.StoredProcedure

            cmd.Parameters.AddWithValue("@ID", CInt(pMaster.val_0))
            cmd.Parameters.AddWithValue("@Item_Code", IIf(String.IsNullOrEmpty(pMaster.val_1), "", pMaster.val_1))
            cmd.Parameters.AddWithValue("@Item_Name", IIf(String.IsNullOrEmpty(pMaster.val_2), "", pMaster.val_2))
            cmd.Parameters.AddWithValue("@Item_cls", IIf(String.IsNullOrEmpty(pMaster.val_3), "", pMaster.val_3))
            cmd.Parameters.AddWithValue("@Section_Cls", IIf(String.IsNullOrEmpty(pMaster.val_4), "", pMaster.val_4))
            cmd.Parameters.AddWithValue("@Group_Cls  ", IIf(String.IsNullOrEmpty(pMaster.val_5), "", pMaster.val_5))
            cmd.Parameters.AddWithValue("@Model_Cls  ", IIf(String.IsNullOrEmpty(pMaster.val_6), "", pMaster.val_6))
            cmd.Parameters.AddWithValue("@External_Code", IIf(String.IsNullOrEmpty(pMaster.val_7), "", pMaster.val_7))
            cmd.Parameters.AddWithValue("@External_Description", IIf(String.IsNullOrEmpty(pMaster.val_8), "", pMaster.val_8))
            cmd.Parameters.AddWithValue("@Spesification", IIf(String.IsNullOrEmpty(pMaster.val_9), "", pMaster.val_9))
            cmd.Parameters.AddWithValue("@UseEndDay", IIf(String.IsNullOrEmpty(pMaster.val_10), "", pMaster.val_10))

            cmd.Parameters.AddWithValue("@MakeOrBuy_Cls", IIf(String.IsNullOrEmpty(pMaster.val_11), "", pMaster.val_11))
            cmd.Parameters.AddWithValue("@Supplier_Code", IIf(String.IsNullOrEmpty(pMaster.val_12), "", pMaster.val_12))
            cmd.Parameters.AddWithValue("@Person_In_Charge_Cls", IIf(String.IsNullOrEmpty(pMaster.val_13), "", pMaster.val_13))
            cmd.Parameters.AddWithValue("@Delivery_Lead_Time", IIf(String.IsNullOrEmpty(pMaster.val_14), "0", pMaster.val_14))
            cmd.Parameters.AddWithValue("@Stock_Control_Cls", IIf(String.IsNullOrEmpty(pMaster.val_15), "", pMaster.val_15))
            cmd.Parameters.AddWithValue("@Warehouse_Code", IIf(String.IsNullOrEmpty(pMaster.val_16), "", pMaster.val_16))
            cmd.Parameters.AddWithValue("@Location_Code", IIf(String.IsNullOrEmpty(pMaster.val_17), "", pMaster.val_17))
            cmd.Parameters.AddWithValue("@Unit_Cls", IIf(String.IsNullOrEmpty(pMaster.val_18), "", pMaster.val_18))

            cmd.Parameters.AddWithValue("@Standard_Stock", IIf(String.IsNullOrEmpty(pMaster.val_19), "0", pMaster.val_19))
            cmd.Parameters.AddWithValue("@Safety_Stock", IIf(String.IsNullOrEmpty(pMaster.val_20), "0", pMaster.val_20))
            cmd.Parameters.AddWithValue("@Max_Stock", IIf(String.IsNullOrEmpty(pMaster.val_21), "0", pMaster.val_21))
            cmd.Parameters.AddWithValue("@Min_Stock", IIf(String.IsNullOrEmpty(pMaster.val_22), "0", pMaster.val_22))
            cmd.Parameters.AddWithValue("@LabelType", IIf(String.IsNullOrEmpty(pMaster.val_23), "", pMaster.val_23))
            cmd.Parameters.AddWithValue("@PrintLabel_Cls", IIf(String.IsNullOrEmpty(pMaster.val_24), "", pMaster.val_24))
            cmd.Parameters.AddWithValue("@UseLot_Cls ", IIf(String.IsNullOrEmpty(pMaster.val_25), "", pMaster.val_25))
            cmd.Parameters.AddWithValue("@Production_Cls", IIf(String.IsNullOrEmpty(pMaster.val_26), "", pMaster.val_26))
            cmd.Parameters.AddWithValue("@Manufacture_Code", IIf(String.IsNullOrEmpty(pMaster.val_27), "", pMaster.val_27))
            cmd.Parameters.AddWithValue("@Line_Code", IIf(String.IsNullOrEmpty(pMaster.val_28), "", pMaster.val_28))
            cmd.Parameters.AddWithValue("@Inspection_Cls", IIf(String.IsNullOrEmpty(pMaster.val_29), "", pMaster.val_29))
            cmd.Parameters.AddWithValue("@Thickness", IIf(String.IsNullOrEmpty(pMaster.val_30), "0", pMaster.val_30))
            cmd.Parameters.AddWithValue("@Width", IIf(String.IsNullOrEmpty(pMaster.val_31), "0", pMaster.val_31))
            cmd.Parameters.AddWithValue("@Length", IIf(String.IsNullOrEmpty(pMaster.val_32), "0", pMaster.val_32))
            cmd.Parameters.AddWithValue("@Net_Weight", IIf(String.IsNullOrEmpty(pMaster.val_33), "0", pMaster.val_33))
            cmd.Parameters.AddWithValue("@Gross_Weight", IIf(String.IsNullOrEmpty(pMaster.val_34), "0", pMaster.val_34))

            cmd.Parameters.AddWithValue("@FIFO_Cls", IIf(String.IsNullOrEmpty(pMaster.val_35), "", pMaster.val_35))
            cmd.Parameters.AddWithValue("@Storage_Type", IIf(String.IsNullOrEmpty(pMaster.val_36), "", pMaster.val_36))
            cmd.Parameters.AddWithValue("@Material_Type", IIf(String.IsNullOrEmpty(pMaster.val_37), "", pMaster.val_37))
            cmd.Parameters.AddWithValue("@ARML", IIf(String.IsNullOrEmpty(pMaster.val_38), "", pMaster.val_38))
            cmd.Parameters.AddWithValue("@Titik_Bakar", IIf(String.IsNullOrEmpty(pMaster.val_39), "", pMaster.val_39))

            cmd.Parameters.AddWithValue("@Address_Code_Unopened", IIf(String.IsNullOrEmpty(pMaster.val_40), "", pMaster.val_40))
            cmd.Parameters.AddWithValue("@Address_Code_Opened", IIf(String.IsNullOrEmpty(pMaster.val_41), "", pMaster.val_41))
            cmd.Parameters.AddWithValue("@SLRS", IIf(String.IsNullOrEmpty(pMaster.val_42), "", pMaster.val_42))
            cmd.Parameters.AddWithValue("@Shelf_Life_Day", IIf(String.IsNullOrEmpty(pMaster.val_43), "0", pMaster.val_43))
            cmd.Parameters.AddWithValue("@HS_Code", IIf(String.IsNullOrEmpty(pMaster.val_44), "", pMaster.val_44))
            cmd.Parameters.AddWithValue("@RMGroup", IIf(String.IsNullOrEmpty(pMaster.val_46), "", pMaster.val_46))
            'cmd.Parameters.AddWithValue("@FDA_No", IIf(String.IsNullOrEmpty(pMaster.val_46), "", pMaster.val_46))
            cmd.Parameters.AddWithValue("@Last_Update", LastUpdate)

            cmd.ExecuteNonQuery()
            cmd.Parameters.Clear()
            cmd.Dispose()

            'SQLTrans.Commit()

            status = True
        Catch ex As Exception
            SQLTrans.Rollback()
            Throw New Exception("sp_OGIScheduler_Item_Master_Ins - Insert Data Error : " & ex.Message)
        Finally
            con.Close()
        End Try
        Return status
    End Function

    Public Shared Function Insert_Trade_Master(ByVal pConStr As String, ByVal pMaster As clsScheduler, Optional ByRef pErr As String = "") As Boolean

        Dim cmd As SqlCommand
        Dim sql As String
        Dim status As Boolean = False
        Dim LastUpdate As DateTime
        Dim con As New SqlConnection
        Dim SQLTrans As SqlTransaction

        con = New SqlConnection(pConStr)
        con.Open()

        'SQLTrans = con.BeginTransaction

        Try
            LastUpdate = DateTime.ParseExact(pMaster.val_16, "MM/dd/yyyy", Nothing)

            sql = "sp_OGIScheduler_Trade_Master_Ins"

            cmd = New SqlCommand
            cmd.CommandText = sql
            cmd.Connection = con
            cmd.Transaction = SQLTrans
            cmd.CommandType = CommandType.StoredProcedure

            cmd.Parameters.AddWithValue("@Trade_Code", IIf(String.IsNullOrEmpty(pMaster.val_1), "", pMaster.val_1))
            cmd.Parameters.AddWithValue("@Trade_Cls", IIf(String.IsNullOrEmpty(pMaster.val_2), "", pMaster.val_2))
            cmd.Parameters.AddWithValue("@TradeExternal_Cls", IIf(String.IsNullOrEmpty(pMaster.val_3), "", pMaster.val_3))
            cmd.Parameters.AddWithValue("@Trade_Name", IIf(String.IsNullOrEmpty(pMaster.val_4), "", pMaster.val_4))
            cmd.Parameters.AddWithValue("@Trade_Abbr", IIf(String.IsNullOrEmpty(pMaster.val_5), "", pMaster.val_5))
            cmd.Parameters.AddWithValue("@Contact_Person", IIf(String.IsNullOrEmpty(pMaster.val_6), "", pMaster.val_6))
            cmd.Parameters.AddWithValue("@Address  ", IIf(String.IsNullOrEmpty(pMaster.val_7), "", pMaster.val_7))
            cmd.Parameters.AddWithValue("@City", IIf(String.IsNullOrEmpty(pMaster.val_8), "", pMaster.val_8))
            cmd.Parameters.AddWithValue("@Postal_Code", IIf(String.IsNullOrEmpty(pMaster.val_9), "", pMaster.val_9))
            cmd.Parameters.AddWithValue("@Telephone", IIf(String.IsNullOrEmpty(pMaster.val_10), "", pMaster.val_10))
            cmd.Parameters.AddWithValue("@Fax", IIf(String.IsNullOrEmpty(pMaster.val_11), "", pMaster.val_11))
            cmd.Parameters.AddWithValue("@Email", IIf(String.IsNullOrEmpty(pMaster.val_12), "", pMaster.val_12))
            cmd.Parameters.AddWithValue("@Country_Cls", IIf(String.IsNullOrEmpty(pMaster.val_13), "", pMaster.val_13))
            cmd.Parameters.AddWithValue("@Country", IIf(String.IsNullOrEmpty(pMaster.val_14), "", pMaster.val_14))
            cmd.Parameters.AddWithValue("@Region_Cls", IIf(String.IsNullOrEmpty(pMaster.val_15), "", pMaster.val_15))
            cmd.Parameters.AddWithValue("@LastUpdate", LastUpdate)

            cmd.ExecuteNonQuery()
            cmd.Parameters.Clear()
            cmd.Dispose()

            'SQLTrans.Commit()
            status = True
        Catch ex As Exception
            SQLTrans.Rollback()
            Throw New Exception("sp_OGIScheduler_Trade_Master_Insn - Insert Data Error : " & ex.Message)
        Finally
            con.Close()
        End Try
        Return status
    End Function

    Public Shared Function Insert_Log(ByVal pConStr As String, ByVal pFile As String, ByVal Msg As String, ByVal ErrorCls As String, Optional ByRef pErr As String = "") As Boolean

        Dim cmd As SqlCommand
        Dim sql As String
        Dim status As Boolean = False
        Dim LastUpdate As DateTime
        Dim con As New SqlConnection
        Dim SQLTrans As SqlTransaction

        con = New SqlConnection(pConStr)
        con.Open()

        'SQLTrans = con.BeginTransaction

        Try

            sql = "sp_OGIScheduler_SchedulerLog_Ins"

            cmd = New SqlCommand
            cmd.CommandText = sql
            cmd.Connection = con
            cmd.Transaction = SQLTrans
            cmd.CommandType = CommandType.StoredProcedure

            cmd.Parameters.AddWithValue("@FileName", IIf(String.IsNullOrEmpty(pFile), "", pFile))
            cmd.Parameters.AddWithValue("@Message", IIf(String.IsNullOrEmpty(Msg), "", Msg))
            cmd.Parameters.AddWithValue("@ErrorCls", IIf(String.IsNullOrEmpty(ErrorCls), "", ErrorCls))

            cmd.ExecuteNonQuery()
            cmd.Parameters.Clear()
            cmd.Dispose()

            'SQLTrans.Commit()
            status = True
        Catch ex As Exception
            SQLTrans.Rollback()
            Throw New Exception("sp_OGIScheduler_SchedulerLog_Ins - Insert Data Error : " & ex.Message)
        Finally
            con.Close()
        End Try
        Return status
    End Function

    Public Shared Function Insert_Receiving_Schedule(ByVal pConStr As String, ByVal pMaster As clsScheduler, Optional ByRef pErr As String = "") As Boolean

        Dim cmd As SqlCommand
        Dim sql As String
        Dim status As Boolean = False
        Dim LastUpdate As DateTime, ReceivedDate As DateTime
        Dim con As New SqlConnection
        Dim SQLTrans As SqlTransaction

        con = New SqlConnection(pConStr)
        con.Open()

        'SQLTrans = con.BeginTransaction
        Try
            LastUpdate = DateTime.ParseExact(pMaster.val_12, "MM/dd/yyyy", Nothing)
            ReceivedDate = DateTime.ParseExact(pMaster.val_3, "MM/dd/yyyy", Nothing)

            sql = "sp_OGIScheduler_Receiving_Schedule_Ins"

            cmd = New SqlCommand
            cmd.CommandText = sql
            cmd.Connection = con
            cmd.Transaction = SQLTrans
            cmd.CommandType = CommandType.StoredProcedure
            cmd.Parameters.AddWithValue("@PO_No", IIf(String.IsNullOrEmpty(pMaster.val_1), "", pMaster.val_1))
            cmd.Parameters.AddWithValue("@Supplier_Code", IIf(String.IsNullOrEmpty(pMaster.val_2), "", pMaster.val_2))
            cmd.Parameters.AddWithValue("@Received_Date", ReceivedDate)
            cmd.Parameters.AddWithValue("@Item_Code", IIf(String.IsNullOrEmpty(pMaster.val_4), "", pMaster.val_4))
            cmd.Parameters.AddWithValue("@Qty_Pack  ", CDbl(IIf(String.IsNullOrEmpty(pMaster.val_5), 0, pMaster.val_5)))
            cmd.Parameters.AddWithValue("@Total_Pack", CDbl(IIf(String.IsNullOrEmpty(pMaster.val_6), 0, pMaster.val_6)))
            cmd.Parameters.AddWithValue("@Qty_Order", CDbl(IIf(String.IsNullOrEmpty(pMaster.val_7), 0, pMaster.val_7)))
            cmd.Parameters.AddWithValue("@Unit", IIf(String.IsNullOrEmpty(pMaster.val_8), "", pMaster.val_8))
            cmd.Parameters.AddWithValue("@Ship_Via", IIf(String.IsNullOrEmpty(pMaster.val_9), "", pMaster.val_9))
            cmd.Parameters.AddWithValue("@SLRS", IIf(String.IsNullOrEmpty(pMaster.val_10), "", pMaster.val_10))
            cmd.Parameters.AddWithValue("@Jenis_Kemasan", IIf(String.IsNullOrEmpty(pMaster.val_11), "", pMaster.val_11))
            cmd.Parameters.AddWithValue("@LastUpdate", LastUpdate)

            cmd.ExecuteNonQuery()
            cmd.Parameters.Clear()
            cmd.Dispose()
            'SQLTrans.Commit()
            status = True
        Catch ex As Exception

            Throw New Exception("sp_OGIScheduler_Receiving_Schedule_Ins - Insert Data Error : " & ex.Message)
            SQLTrans.Rollback()
        Finally
            con.Close()
        End Try
        Return status
    End Function
    Public Shared Function Insert_Delivery_Ins(ByVal pConStr As String, ByVal pMaster As clsScheduler, Optional ByRef pErr As String = "") As Boolean

        Dim cmd As New SqlCommand
        Dim sql As String
        Dim status As Boolean = False
        Dim LastUpdate As DateTime, OrderDate As DateTime, PickupDate As DateTime
        Dim con As New SqlConnection
        Dim SQLTrans As SqlTransaction

        con = New SqlConnection(pConStr)
        con.Open()

        'SQLTrans = con.BeginTransaction
        Try
            OrderDate = DateTime.ParseExact(pMaster.val_5, "MM/dd/yyyy", Nothing)
            PickupDate = DateTime.ParseExact(pMaster.val_6, "MM/dd/yyyy", Nothing)
            LastUpdate = DateTime.ParseExact(pMaster.val_16, "MM/dd/yyyy", Nothing)

            sql = "sp_OGIScheduler_Delivery_Instruction_Ins"

            cmd = New SqlCommand
            cmd.CommandText = sql
            cmd.Connection = con
            cmd.Transaction = SQLTrans
            cmd.CommandType = CommandType.StoredProcedure

            cmd.Parameters.AddWithValue("@id", IIf(String.IsNullOrEmpty(pMaster.val_0), "", pMaster.val_0))
            cmd.Parameters.AddWithValue("@Shipper_ID", IIf(String.IsNullOrEmpty(pMaster.val_1), "", pMaster.val_1))
            cmd.Parameters.AddWithValue("@Order_No", IIf(String.IsNullOrEmpty(pMaster.val_2), "", pMaster.val_2))
            cmd.Parameters.AddWithValue("@Order_Type", IIf(String.IsNullOrEmpty(pMaster.val_3), "", pMaster.val_3))
            cmd.Parameters.AddWithValue("@Customer_ID", IIf(String.IsNullOrEmpty(pMaster.val_4), "", pMaster.val_4))
            cmd.Parameters.AddWithValue("@Order_Date", OrderDate)
            cmd.Parameters.AddWithValue("@Pickup_Date  ", PickupDate)
            cmd.Parameters.AddWithValue("@Customer_PO_No", IIf(String.IsNullOrEmpty(pMaster.val_7), 0, pMaster.val_7))
            cmd.Parameters.AddWithValue("@Site_ID", IIf(String.IsNullOrEmpty(pMaster.val_8), 0, pMaster.val_8))
            cmd.Parameters.AddWithValue("@Line_ID", IIf(String.IsNullOrEmpty(pMaster.val_9), "", pMaster.val_9))
            cmd.Parameters.AddWithValue("@Item_Code", IIf(String.IsNullOrEmpty(pMaster.val_10), "", pMaster.val_10))
            cmd.Parameters.AddWithValue("@Order_Qty", CDbl(IIf(String.IsNullOrEmpty(pMaster.val_11), "0", pMaster.val_11)))
            cmd.Parameters.AddWithValue("@Unit", IIf(String.IsNullOrEmpty(pMaster.val_12), "", pMaster.val_12))
            cmd.Parameters.AddWithValue("@Shipped_Qty", CDbl(IIf(String.IsNullOrEmpty(pMaster.val_13), "0", pMaster.val_13)))
            cmd.Parameters.AddWithValue("@WBL", IIf(String.IsNullOrEmpty(pMaster.val_14), "", pMaster.val_14))
            cmd.Parameters.AddWithValue("@Lot_No", IIf(String.IsNullOrEmpty(pMaster.val_15), "", pMaster.val_15))
            cmd.Parameters.AddWithValue("@LastUpdate", LastUpdate)

            cmd.ExecuteNonQuery()
            cmd.Parameters.Clear()
            cmd.Dispose()
            'SQLTrans.Commit()
            status = True
        Catch ex As Exception
            SQLTrans.Rollback()
            Throw New Exception("sp_OGIScheduler_Delivery_Instruction_Ins - Insert Data Error : " & ex.Message)
        Finally
            con.Close()
        End Try
        Return status
    End Function
    Public Shared Function Insert_Manufacture_Book(ByVal pConStr As String, ByVal pMaster As clsScheduler, Optional ByRef pErr As String = "") As Boolean

        Dim cmd As SqlCommand
        Dim sql As String
        Dim status As Boolean = False
        Dim LastUpdate As DateTime, StartDate As DateTime, EndDate As Date, ProductionDate As Date
        Dim ShipperDate As String = ""
        Dim con As New SqlConnection
        Dim SQLTrans As SqlTransaction

        con = New SqlConnection(pConStr)
        con.Open()

        'SQLTrans = con.BeginTransaction
        Try
            LastUpdate = DateTime.ParseExact(pMaster.val_19, "MM/dd/yyyy", Nothing)
            StartDate = DateTime.ParseExact(pMaster.val_2, "MM/dd/yyyy", Nothing)
            EndDate = DateTime.ParseExact(pMaster.val_3, "MM/dd/yyyy", Nothing)

            If pMaster.val_10 <> "" Then
                ShipperDate = DateTime.ParseExact(pMaster.val_10, "MM/dd/yyyy", Nothing).ToString("yyyy-MM-dd")
            End If

            sql = "sp_OGIScheduler_Manufacture_Book_Ins"

            cmd = New SqlCommand
            cmd.CommandText = sql
            cmd.Connection = con
            cmd.Transaction = SQLTrans
            cmd.CommandType = CommandType.StoredProcedure
            cmd.Parameters.AddWithValue("@ID", IIf(String.IsNullOrEmpty(pMaster.val_0), 0, pMaster.val_0))
            cmd.Parameters.AddWithValue("@Production_ID", IIf(String.IsNullOrEmpty(pMaster.val_1), "", pMaster.val_1))
            cmd.Parameters.AddWithValue("@Start_Date", StartDate)
            cmd.Parameters.AddWithValue("@End_Date", EndDate)
            cmd.Parameters.AddWithValue("@Manufacture_Code", IIf(String.IsNullOrEmpty(pMaster.val_4), "", pMaster.val_4))
            cmd.Parameters.AddWithValue("@Line_Code", IIf(String.IsNullOrEmpty(pMaster.val_5), "", pMaster.val_5))
            cmd.Parameters.AddWithValue("@Product_Code", IIf(String.IsNullOrEmpty(pMaster.val_6), "", pMaster.val_6))
            cmd.Parameters.AddWithValue("@Commodity_Name", IIf(String.IsNullOrEmpty(pMaster.val_7), "", pMaster.val_7))
            cmd.Parameters.AddWithValue("@Product_Lot_No", IIf(String.IsNullOrEmpty(pMaster.val_8), "", pMaster.val_8))
            cmd.Parameters.AddWithValue("@Target_Qty", IIf(String.IsNullOrEmpty(pMaster.val_9), "", pMaster.val_9))
            cmd.Parameters.AddWithValue("@Shipper_Date  ", ShipperDate)
            'cmd.Parameters.AddWithValue("@Package", IIf(String.IsNullOrEmpty(pMaster.val_11), "", pMaster.val_11))
            'cmd.Parameters.AddWithValue("@Address", IIf(String.IsNullOrEmpty(pMaster.val_12), "", pMaster.val_12))
            cmd.Parameters.AddWithValue("@LineID", IIf(String.IsNullOrEmpty(pMaster.val_11), "", pMaster.val_11))
            cmd.Parameters.AddWithValue("@RM_Category", IIf(String.IsNullOrEmpty(pMaster.val_12), "", pMaster.val_12))
            cmd.Parameters.AddWithValue("@Item_Code", IIf(String.IsNullOrEmpty(pMaster.val_13), "", pMaster.val_13))
            cmd.Parameters.AddWithValue("@Item_Lot_No", IIf(String.IsNullOrEmpty(pMaster.val_14), "", pMaster.val_14))
            cmd.Parameters.AddWithValue("@Qty_Kg", IIf(String.IsNullOrEmpty(pMaster.val_15), "0", pMaster.val_15))
            cmd.Parameters.AddWithValue("@Qty_Use", IIf(String.IsNullOrEmpty(pMaster.val_16), "0", pMaster.val_16))
            cmd.Parameters.AddWithValue("@Unit", IIf(String.IsNullOrEmpty(pMaster.val_17), "", pMaster.val_17))
            cmd.Parameters.AddWithValue("@Last_Update", LastUpdate)
            cmd.Parameters.AddWithValue("@Item_Code_SL", IIf(String.IsNullOrEmpty(pMaster.val_50), "", pMaster.val_50))
            cmd.Parameters.AddWithValue("@MaterialLineID", IIf(String.IsNullOrEmpty(pMaster.val_18), "", pMaster.val_18))

            cmd.ExecuteNonQuery()
            cmd.Parameters.Clear()
            cmd.Dispose()
            'SQLTrans.Commit()
            status = True
        Catch ex As Exception
            Throw New Exception("sp_OGIScheduler_Receiving_Schedule_Ins - Insert Data Error : " & ex.Message)
            SQLTrans.Rollback()
        Finally
            con.Close()
        End Try
        Return status
    End Function
    Public Shared Function Insert_Picking_List(ByVal pConStr As String, ByVal pMaster As clsScheduler, Optional ByRef pErr As String = "") As Boolean

        Dim cmd As SqlCommand
        Dim sql As String
        Dim status As Boolean = False
        Dim LastUpdate As DateTime, ProductionDate As DateTime
        Dim con As New SqlConnection
        Dim SQLTrans As SqlTransaction

        con = New SqlConnection(pConStr)
        con.Open()

        'SQLTrans = con.BeginTransaction
        Try
            LastUpdate = DateTime.ParseExact(pMaster.val_18, "MM/dd/yyyy", Nothing)
            ProductionDate = DateTime.ParseExact(pMaster.val_2, "MM/dd/yyyy", Nothing)

            sql = "sp_OGIScheduler_Picking_List_Ins"

            cmd = New SqlCommand
            cmd.CommandText = sql
            cmd.Connection = con
            'cmd.Transaction = SQLTrans
            cmd.CommandType = CommandType.StoredProcedure
            cmd.Parameters.AddWithValue("@ID", IIf(String.IsNullOrEmpty(pMaster.val_0), "", pMaster.val_0))
            cmd.Parameters.AddWithValue("@Production_ID", IIf(String.IsNullOrEmpty(pMaster.val_1), "", pMaster.val_1))
            cmd.Parameters.AddWithValue("@Production_Date", Format(ProductionDate, "yyyy-MM-dd"))
            cmd.Parameters.AddWithValue("@Product_Code", IIf(String.IsNullOrEmpty(pMaster.val_3), "", pMaster.val_3))
            cmd.Parameters.AddWithValue("@STATUS", IIf(String.IsNullOrEmpty(pMaster.val_4), "", pMaster.val_4))
            cmd.Parameters.AddWithValue("@Manufacture_Code", IIf(String.IsNullOrEmpty(pMaster.val_5), "", pMaster.val_5))
            cmd.Parameters.AddWithValue("@Line_Code", IIf(String.IsNullOrEmpty(pMaster.val_6), "", pMaster.val_6))
            cmd.Parameters.AddWithValue("@Product_Lot_No", IIf(String.IsNullOrEmpty(pMaster.val_7), "", pMaster.val_7))
            cmd.Parameters.AddWithValue("@Site", IIf(String.IsNullOrEmpty(pMaster.val_8), "", pMaster.val_8))
            cmd.Parameters.AddWithValue("@Address", IIf(String.IsNullOrEmpty(pMaster.val_9), "", pMaster.val_9))
            cmd.Parameters.AddWithValue("@RM_Category", IIf(String.IsNullOrEmpty(pMaster.val_10), "", pMaster.val_10))
            cmd.Parameters.AddWithValue("@Item_Code", IIf(String.IsNullOrEmpty(pMaster.val_11), "", pMaster.val_11))
            cmd.Parameters.AddWithValue("@Item_Lot_No", IIf(String.IsNullOrEmpty(pMaster.val_12), "", pMaster.val_12))
            cmd.Parameters.AddWithValue("@Package", IIf(String.IsNullOrEmpty(pMaster.val_13), "", pMaster.val_13))
            cmd.Parameters.AddWithValue("@Description", IIf(String.IsNullOrEmpty(pMaster.val_14), "", pMaster.val_14))
            cmd.Parameters.AddWithValue("@Qty_Use", IIf(String.IsNullOrEmpty(pMaster.val_15), 0, pMaster.val_15))
            cmd.Parameters.AddWithValue("@Unit", IIf(String.IsNullOrEmpty(pMaster.val_16), "", pMaster.val_16))
            cmd.Parameters.AddWithValue("@LastUpdate", LastUpdate)
            cmd.Parameters.AddWithValue("@MaterialLineID", IIf(String.IsNullOrEmpty(pMaster.val_17), "", pMaster.val_17))
            cmd.ExecuteNonQuery()
            cmd.Parameters.Clear()
            cmd.Dispose()

            'SQLTrans.Commit()

            status = True
        Catch ex As Exception
            SQLTrans.Rollback()
            Throw New Exception("sp_OGIScheduler_Receiving_Schedule_Ins - Insert Data Error : " & ex.Message)
        Finally
            con.Close()
        End Try
        Return status
    End Function

    Public Shared Function PickingList_Validasi(ByVal pConStr As String, Optional ByRef pErr As String = "") As String

        Dim cmd As SqlCommand
        Dim sql As String
        Dim da As SqlDataAdapter
        Dim dt As New DataTable

        Try
            Using connection As New SqlConnection(pConStr)
                connection.Open()
                sql = "sp_OGIScheduler_PickingList_Verification"
                cmd = New SqlCommand(sql, connection)
                cmd.CommandType = CommandType.StoredProcedure
                da = New SqlDataAdapter(cmd)
                da.Fill(dt)
                pErr = dt.Rows(0)("RESPONSE")
                Return pErr
            End Using
        Catch ex As Exception
            Throw New Exception("sp_OGIScheduler_PickingList_Verification - Insert Data Error : " & ex.Message)
        End Try
    End Function

    Public Shared Function Insert_Physical_Inventory(ByVal pConStr As String, ByVal pMaster As clsScheduler, Optional ByRef pErr As String = "") As Boolean

        Dim cmd As SqlCommand
        Dim sql As String
        Dim status As Boolean = False
        Dim LastUpdate As DateTime, ProductionDate As DateTime
        Dim con As New SqlConnection
        Dim SQLTrans As SqlTransaction

        con = New SqlConnection(pConStr)
        con.Open()

        'SQLTrans = con.BeginTransaction
        Try
            LastUpdate = DateTime.ParseExact(pMaster.val_8, "MM/dd/yyyy", Nothing)

            sql = "sp_OGIScheduler_Physical_Inventory_Ins"

            cmd = New SqlCommand
            cmd.CommandText = sql
            cmd.Connection = con
            cmd.Transaction = SQLTrans
            cmd.CommandType = CommandType.StoredProcedure
            cmd.Parameters.AddWithValue("@ID", IIf(String.IsNullOrEmpty(pMaster.val_0), "", pMaster.val_0))
            cmd.Parameters.AddWithValue("@Period", IIf(String.IsNullOrEmpty(pMaster.val_1), "", pMaster.val_1))
            cmd.Parameters.AddWithValue("@SiteID", IIf(String.IsNullOrEmpty(pMaster.val_2), "", pMaster.val_2))
            cmd.Parameters.AddWithValue("@Address", IIf(String.IsNullOrEmpty(pMaster.val_3), "", pMaster.val_3))
            cmd.Parameters.AddWithValue("@Item_Code", IIf(String.IsNullOrEmpty(pMaster.val_4), "", pMaster.val_4))
            cmd.Parameters.AddWithValue("@Qty  ", CDbl(IIf(String.IsNullOrEmpty(pMaster.val_5), 0, pMaster.val_5)))
            cmd.Parameters.AddWithValue("@Unit", IIf(String.IsNullOrEmpty(pMaster.val_6), "", pMaster.val_6))
            cmd.Parameters.AddWithValue("@Lot_No", IIf(String.IsNullOrEmpty(pMaster.val_7), "", pMaster.val_7))
            cmd.Parameters.AddWithValue("@Last_Update", LastUpdate)
            cmd.Parameters.AddWithValue("@Physical_Count_ID", IIf(String.IsNullOrEmpty(pMaster.val_9), "", pMaster.val_9))
            cmd.Parameters.AddWithValue("@Tag_Number", CDbl(IIf(String.IsNullOrEmpty(pMaster.val_10), 0, pMaster.val_10)))
            cmd.ExecuteNonQuery()
            cmd.Parameters.Clear()
            cmd.Dispose()
            'SQLTrans.Commit()
            status = True
        Catch ex As Exception
            SQLTrans.Rollback()
            Throw New Exception("sp_OGIScheduler_Receiving_Schedule_Ins - Insert Data Error : " & ex.Message)
        Finally
            con.Close()
        End Try
        Return status
    End Function

    Public Shared Function Export_Sel(ByVal iFlag As Integer, ByVal pConStr As String, Optional ByVal pErr As String = "") As DataTable
        Dim sql As String
        Dim cmd As SqlCommand
        Dim da As SqlDataAdapter
        Dim dt As New DataTable

        Try
            Using connection As New SqlConnection(pConStr)
                connection.Open()

                sql = "sp_OGIScheduler_Export_Sel"

                cmd = New SqlCommand(sql, connection)
                cmd.CommandType = CommandType.StoredProcedure
                cmd.Parameters.AddWithValue("@Flag", iFlag)
                da = New SqlDataAdapter(cmd)
                da.Fill(dt)

                Return dt

            End Using
        Catch ex As Exception
            Throw New Exception("GET DATA RECEIVING RESULT ERROR : " & ex.Message)
        End Try
    End Function

    Public Shared Function Scheduler_Upd(ByVal iFlag As Integer, ByVal pConStr As String) As Boolean
        Dim retValue As Integer = 0
        Dim sql As String = ""
        Dim cmd As SqlCommand
        Dim status As Boolean = False
        Dim con As New SqlConnection
        Dim SQLTrans As SqlTransaction

        con = New SqlConnection(pConStr)
        con.Open()

        'SQLTrans = con.BeginTransaction
        Try
            sql = "sp_OGIScheduler_Upd"

            cmd = New SqlCommand
            cmd.CommandText = sql
            cmd.Connection = con
            cmd.Transaction = SQLTrans
            cmd.CommandType = CommandType.StoredProcedure
            cmd.Parameters.AddWithValue("@Flag", iFlag)
            cmd.ExecuteNonQuery()
            cmd.Parameters.Clear()
            cmd.Dispose()
            status = True

            'SQLTrans.Commit()

        Catch ex As Exception
            SQLTrans.Rollback()
            Throw New Exception("Scheduler- Update Data Error : " & ex.Message)
        Finally
            con.Close()
        End Try


        Return status
    End Function

    Public Shared Function Check_AddressTo(ByVal pConStr As String) As DataTable
        Dim sql As String
        Dim cmd As SqlCommand
        Dim da As SqlDataAdapter
        Dim dt As New DataTable

        Try
            Using connection As New SqlConnection(pConStr)
                connection.Open()

                sql = "SP_OGIScheduler_AddressTo_Val"

                cmd = New SqlCommand(sql, connection)
                cmd.CommandType = CommandType.StoredProcedure
                da = New SqlDataAdapter(cmd)
                da.Fill(dt)
                Return dt
            End Using
        Catch ex As Exception
            Throw New Exception("GET DATA RECEIVING RESULT ERROR : " & ex.Message)
        End Try
    End Function

End Class
