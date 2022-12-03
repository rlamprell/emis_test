
from src.extract            import Extract
from src.transform          import Transform
from src.mySQL_connection   import mySQL_connection



def main():
    raw_files           = Extract(folder_name="test_data").getFiles()
    # # unpacked_files  = Transform(raw_files).unpack_by_loop()

    file_transformer    = Transform()
    unpacked_files      = file_transformer.unpack_by_map(raw_files)
    seperated_files     = file_transformer.seperate_by_uniqueness_map(unpacked_files, 'resource.resourceType')

    for df in seperated_files:
        seperated_files[df] = file_transformer.exploder(seperated_files[df], df)
        seperated_files[df] = file_transformer.flatten_df(seperated_files[df])
        print(df)
        print(seperated_files[df].columns.tolist())

        seperated_files[df]  = seperated_files[df].drop_duplicates()

        mySQL_connection("conn").post(df, seperated_files[df])



        # if df=='Patient':
        #     break


def main2():
    raw_files           = Extract(files=1).getFiles()
    # # unpacked_files  = Transform(raw_files).unpack_by_loop()

    file_transformer    = Transform()
    unpacked_files      = file_transformer.unpack_by_map(raw_files, 'entry')
    print(f"\n\n\n\n\n")
    print("unpacked files")
    print(unpacked_files.columns.tolist())
    seperated_files     = file_transformer.seperate_by_uniqueness_map(unpacked_files, 'resource.resourceType')
    
    print(f"\n\n\n\n\n")
    print("seperated files")
    print(seperated_files['Patient'].columns.tolist())

    print(f"\n\n\n\n\n")
    for df in seperated_files:
        print(seperated_files[df])
        print(seperated_files[df]["resource.extension"])
        print(type(seperated_files[df]["resource.extension"][0]))

        # check the first item
        this_type = seperated_files[df]["resource.extension"][0]
        print(type(this_type))
        if isinstance(this_type, list):
            print("is list")
            print(seperated_files[df]["resource.extension"][0])

            

        # seperated_files[df] = exploder(seperated_files[df], df)
        # seperated_files[df] = file_transformer.flatten_df(seperated_files[df])
        # print(df)
        # print(seperated_files[df].columns.tolist())

        # mySQL_connection("conn").post(df, seperated_files[df])



        if df=='Patient':
            break

if __name__ == '__main__':
    main()